const BLOCK_BYTES: usize = 64;
const DIGEST_BYTES: usize = 32;

const INITIAL_STATE: [u32; 8] = [
    0x6a09_e667,
    0xbb67_ae85,
    0x3c6e_f372,
    0xa54f_f53a,
    0x510e_527f,
    0x9b05_688c,
    0x1f83_d9ab,
    0x5be0_cd19,
];

const ROUND_CONSTANTS: [u32; 64] = [
    0x428a_2f98,
    0x7137_4491,
    0xb5c0_fbcf,
    0xe9b5_dba5,
    0x3956_c25b,
    0x59f1_11f1,
    0x923f_82a4,
    0xab1c_5ed5,
    0xd807_aa98,
    0x1283_5b01,
    0x2431_85be,
    0x550c_7dc3,
    0x72be_5d74,
    0x80de_b1fe,
    0x9bdc_06a7,
    0xc19b_f174,
    0xe49b_69c1,
    0xefbe_4786,
    0x0fc1_9dc6,
    0x240c_a1cc,
    0x2de9_2c6f,
    0x4a74_84aa,
    0x5cb0_a9dc,
    0x76f9_88da,
    0x983e_5152,
    0xa831_c66d,
    0xb003_27c8,
    0xbf59_7fc7,
    0xc6e0_0bf3,
    0xd5a7_9147,
    0x06ca_6351,
    0x1429_2967,
    0x27b7_0a85,
    0x2e1b_2138,
    0x4d2c_6dfc,
    0x5338_0d13,
    0x650a_7354,
    0x766a_0abb,
    0x81c2_c92e,
    0x9272_2c85,
    0xa2bf_e8a1,
    0xa81a_664b,
    0xc24b_8b70,
    0xc76c_51a3,
    0xd192_e819,
    0xd699_0624,
    0xf40e_3585,
    0x106a_a070,
    0x19a4_c116,
    0x1e37_6c08,
    0x2748_774c,
    0x34b0_bcb5,
    0x391c_0cb3,
    0x4ed8_aa4a,
    0x5b9c_ca4f,
    0x682e_6ff3,
    0x748f_82ee,
    0x78a5_636f,
    0x84c8_7814,
    0x8cc7_0208,
    0x90be_fffa,
    0xa450_6ceb,
    0xbef9_a3f7,
    0xc671_78f2,
];

/// Fixed HMAC-SHA256 key schedule used only by the Binance testnet signer.
///
/// This small one-shot primitive keeps the repository's no-new-dependency
/// contract. Mainnet authority remains disabled and must not reuse this module
/// without a separately approved cryptography dependency and security review.
#[derive(Clone)]
pub(crate) struct HmacSha256Key {
    block: [u8; BLOCK_BYTES],
}

impl HmacSha256Key {
    pub(crate) fn new(secret: &[u8]) -> Self {
        let normalized = if secret.len() > BLOCK_BYTES {
            sha256(secret).to_vec()
        } else {
            secret.to_vec()
        };
        let mut block = [0_u8; BLOCK_BYTES];
        block[..normalized.len()].copy_from_slice(&normalized);
        Self { block }
    }

    pub(crate) fn sign(&self, payload: &[u8]) -> [u8; DIGEST_BYTES] {
        let mut inner = Vec::with_capacity(BLOCK_BYTES.saturating_add(payload.len()));
        inner.extend(self.block.iter().map(|byte| byte ^ 0x36));
        inner.extend_from_slice(payload);
        let inner_digest = sha256(&inner);

        let mut outer = [0_u8; BLOCK_BYTES + DIGEST_BYTES];
        for (target, key) in outer[..BLOCK_BYTES].iter_mut().zip(self.block) {
            *target = key ^ 0x5c;
        }
        outer[BLOCK_BYTES..].copy_from_slice(&inner_digest);
        sha256(&outer)
    }
}

fn sha256(message: &[u8]) -> [u8; DIGEST_BYTES] {
    let bit_length = u64::try_from(message.len())
        .expect("an addressable message length must fit in u64")
        .checked_mul(8)
        .expect("an addressable message bit length must fit in u64");
    let padded_length = message
        .len()
        .checked_add(1 + std::mem::size_of::<u64>())
        .expect("an addressable padded message length must fit in usize")
        .div_ceil(BLOCK_BYTES)
        .checked_mul(BLOCK_BYTES)
        .expect("an addressable padded message length must fit in usize");
    let mut padded = vec![0_u8; padded_length];
    padded[..message.len()].copy_from_slice(message);
    padded[message.len()] = 0x80;
    padded[padded_length - 8..].copy_from_slice(&bit_length.to_be_bytes());

    let mut state = INITIAL_STATE;
    for block in padded.chunks_exact(BLOCK_BYTES) {
        compress(&mut state, block);
    }

    let mut digest = [0_u8; DIGEST_BYTES];
    for (output, word) in digest.chunks_exact_mut(4).zip(state) {
        output.copy_from_slice(&word.to_be_bytes());
    }
    digest
}

#[allow(clippy::many_single_char_names)]
fn compress(state: &mut [u32; 8], block: &[u8]) {
    let mut schedule = [0_u32; 64];
    for (index, word) in schedule[..16].iter_mut().enumerate() {
        let offset = index * 4;
        *word = u32::from_be_bytes([
            block[offset],
            block[offset + 1],
            block[offset + 2],
            block[offset + 3],
        ]);
    }
    let mut index = 16;
    while index < schedule.len() {
        let s0 = schedule[index - 15].rotate_right(7)
            ^ schedule[index - 15].rotate_right(18)
            ^ (schedule[index - 15] >> 3);
        let s1 = schedule[index - 2].rotate_right(17)
            ^ schedule[index - 2].rotate_right(19)
            ^ (schedule[index - 2] >> 10);
        schedule[index] = schedule[index - 16]
            .wrapping_add(s0)
            .wrapping_add(schedule[index - 7])
            .wrapping_add(s1);
        index += 1;
    }

    let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = *state;
    for (word, constant) in schedule.into_iter().zip(ROUND_CONSTANTS) {
        let sum1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
        let choice = (e & f) ^ ((!e) & g);
        let temporary1 = h
            .wrapping_add(sum1)
            .wrapping_add(choice)
            .wrapping_add(constant)
            .wrapping_add(word);
        let sum0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
        let majority = (a & b) ^ (a & c) ^ (b & c);
        let temporary2 = sum0.wrapping_add(majority);

        h = g;
        g = f;
        f = e;
        e = d.wrapping_add(temporary1);
        d = c;
        c = b;
        b = a;
        a = temporary1.wrapping_add(temporary2);
    }

    for (target, value) in state.iter_mut().zip([a, b, c, d, e, f, g, h]) {
        *target = target.wrapping_add(value);
    }
}

#[cfg(test)]
mod tests {
    use super::{HmacSha256Key, sha256};

    fn hex(bytes: &[u8]) -> String {
        const DIGITS: &[u8; 16] = b"0123456789abcdef";
        let mut output = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            output.push(char::from(DIGITS[usize::from(byte >> 4)]));
            output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
        }
        output
    }

    #[test]
    fn sha256_matches_the_empty_and_abc_vectors() {
        assert_eq!(
            hex(&sha256(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            hex(&sha256(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    /// Byte `i` of the test pattern is `i % 251`.
    ///
    /// 251 is prime, so the pattern never aligns with the 64-byte block and a
    /// block-indexing mistake cannot be masked by repeating bytes.
    fn pattern(length: usize) -> Vec<u8> {
        (0..length)
            .map(|index| u8::try_from(index % 251).expect("251 fits in a byte"))
            .collect()
    }

    #[test]
    fn sha256_matches_the_nist_two_block_vector() {
        assert_eq!(
            hex(&sha256(
                b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"
            )),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
    }

    #[test]
    fn sha256_matches_vectors_across_every_padding_boundary() {
        // A SHA-256 block is 64 bytes and the trailing length field takes 8 of
        // them, so a 55-byte message pads into its own final block while a
        // 56-byte one needs an extra block. Getting that split wrong is the
        // classic implementation bug, and this code signs live orders.
        for (length, expected) in [
            (
                0,
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            ),
            (
                1,
                "6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d",
            ),
            (
                54,
                "675f28acc0b90a72d1c3a570fe83ac565555db358cf01826dc8eefb2bf7ca0f3",
            ),
            (
                55,
                "463eb28e72f82e0a96c0a4cc53690c571281131f672aa229e0d45ae59b598b59",
            ),
            (
                56,
                "da2ae4d6b36748f2a318f23e7ab1dfdf45acdc9d049bd80e59de82a60895f562",
            ),
            (
                57,
                "2fe741af801cc238602ac0ec6a7b0c3a8a87c7fc7d7f02a3fe03d1c12eac4d8f",
            ),
            (
                63,
                "29af2686fd53374a36b0846694cc342177e428d1647515f078784d69cdb9e488",
            ),
            (
                64,
                "fdeab9acf3710362bd2658cdc9a29e8f9c757fcf9811603a8c447cd1d9151108",
            ),
            (
                65,
                "4bfd2c8b6f1eec7a2afeb48b934ee4b2694182027e6d0fc075074f2fabb31781",
            ),
            (
                118,
                "d32ab00929cb935b79d44e74c5a745db460ff794dea3b79be40c1cc5cf5388ef",
            ),
            (
                119,
                "da18797ed7c3a777f0847f429724a2d8cd5138e6ed2895c3fa1a6d39d18f7ec6",
            ),
            (
                120,
                "f52b23db1fbb6ded89ef42a23ce0c8922c45f25c50b568a93bf1c075420bbb7c",
            ),
            (
                127,
                "92ca0fa6651ee2f97b884b7246a562fa71250fedefe5ebf270d31c546bfea976",
            ),
            (
                128,
                "471fb943aa23c511f6f72f8d1652d9c880cfa392ad80503120547703e56a2be5",
            ),
            (
                129,
                "5099c6a56203f9687f7d33f4bfdf576d31dc91f6b695ecea38b2770c87631135",
            ),
        ] {
            assert_eq!(
                hex(&sha256(&pattern(length))),
                expected,
                "digest diverges at message length {length}"
            );
        }
    }

    #[test]
    fn sha256_matches_a_reference_sweep_of_every_length_through_two_blocks() {
        // Chaining every digest from 0 to 130 bytes into one value covers the
        // lengths the explicit table above skips. The expected value comes
        // from a reference implementation.
        let mut chained = Vec::new();
        for length in 0..=130 {
            chained.extend_from_slice(&sha256(&pattern(length)));
        }

        assert_eq!(
            hex(&sha256(&chained)),
            "e5bbbecd60c3632a3455f465bfd8b079c30ef608d2bcc34227f4e5573029020e"
        );
    }

    #[test]
    fn hmac_matches_vectors_across_every_key_padding_boundary() {
        // A key shorter than the 64-byte block is zero-padded, a longer one is
        // hashed first. The behaviour at exactly 64 and 65 bytes is where that
        // branch is usually written wrong.
        for (key_length, expected) in [
            (
                1,
                "601b92f9be6bbfda9873ac429250c809575b003391593355942fff9458e7c683",
            ),
            (
                63,
                "811e68796718b3c068f89841500aadb1b3e7ad2e595867c36799ca3bb6c8d126",
            ),
            (
                64,
                "f54709169dd410b71da4edd693af44e2dacb4d44cfa2f6dd0be6aaf809926e82",
            ),
            (
                65,
                "ed9fc29b21ac6a002c1437078593c39aeb8d232ca1f1fbed76ba3946b30512ce",
            ),
            (
                131,
                "e2dc31503ce1317233cae992fd4a9d3eec9f0edcbc66a00544403de10198053a",
            ),
        ] {
            let key = HmacSha256Key::new(&vec![0x0b; key_length]);
            assert_eq!(
                hex(&key.sign(b"boundary message")),
                expected,
                "signature diverges at key length {key_length}"
            );
        }
    }

    #[test]
    fn hmac_matches_rfc_4231_short_and_long_key_vectors() {
        let short = HmacSha256Key::new(&[0x0b; 20]);
        assert_eq!(
            hex(&short.sign(b"Hi There")),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );

        let long = HmacSha256Key::new(&[0xaa; 131]);
        assert_eq!(
            hex(&long.sign(b"Test Using Larger Than Block-Size Key - Hash Key First")),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
    }
}
