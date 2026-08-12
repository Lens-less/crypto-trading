use crypto_trading_domain::sha256_digest;

/// Fixed HMAC-SHA256 key schedule used only by the Binance testnet signer.
///
/// This small one-shot primitive keeps the repository's no-new-dependency
/// contract. Mainnet authority remains disabled and must not reuse this module
/// without a separately approved cryptography dependency and security review.
const BLOCK_BYTES: usize = 64;
const DIGEST_BYTES: usize = 32;

#[derive(Clone)]
pub(crate) struct HmacSha256Key {
    block: [u8; BLOCK_BYTES],
}

impl HmacSha256Key {
    pub(crate) fn new(secret: &[u8]) -> Self {
        let mut normalized = if secret.len() > BLOCK_BYTES {
            sha256_digest(secret).to_vec()
        } else {
            secret.to_vec()
        };
        let mut block = [0_u8; BLOCK_BYTES];
        block[..normalized.len()].copy_from_slice(&normalized);
        normalized.fill(0);
        Self { block }
    }

    pub(crate) fn sign(&self, payload: &[u8]) -> [u8; DIGEST_BYTES] {
        let mut inner = Vec::with_capacity(BLOCK_BYTES.saturating_add(payload.len()));
        inner.extend(self.block.iter().map(|byte| byte ^ 0x36));
        inner.extend_from_slice(payload);
        let mut inner_digest = sha256_digest(&inner);
        inner.fill(0);

        let mut outer = [0_u8; BLOCK_BYTES + DIGEST_BYTES];
        for (target, key) in outer[..BLOCK_BYTES].iter_mut().zip(self.block) {
            *target = key ^ 0x5c;
        }
        outer[BLOCK_BYTES..].copy_from_slice(&inner_digest);
        let digest = sha256_digest(&outer);
        inner_digest.fill(0);
        outer.fill(0);
        digest
    }
}

impl Drop for HmacSha256Key {
    fn drop(&mut self) {
        self.block.fill(0);
    }
}

#[cfg(test)]
mod tests {
    use super::HmacSha256Key;

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

    #[test]
    fn scratch_zeroization_does_not_corrupt_repeat_signing() {
        let key = HmacSha256Key::new(b"test-secret");
        let first = key.sign(b"payload");
        let second = key.sign(b"payload");
        assert_eq!(first, second);
    }
}
