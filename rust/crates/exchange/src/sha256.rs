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
