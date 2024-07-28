// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! xxhash64 implementation

const CHUNK_SIZE: usize = 32;

const PRIME_1: u64 = 11_400_714_785_074_694_791;
const PRIME_2: u64 = 14_029_467_366_897_019_727;
const PRIME_3: u64 = 1_609_587_929_392_839_161;
const PRIME_4: u64 = 9_650_029_242_287_828_579;
const PRIME_5: u64 = 2_870_177_450_012_600_261;

/// Custom implementation of xxhash64 based on code from https://github.com/shepmaster/twox-hash
/// but optimized for our use case by removing any intermediate buffering, which is
/// not required because we are operating on data that is already in memory.
#[inline]
pub(crate) fn spark_compatible_xxhash64<T: AsRef<[u8]>>(data: T, seed: u64) -> u64 {
    let data: &[u8] = data.as_ref();
    let length_bytes = data.len();

    let mut v1 = seed.wrapping_add(PRIME_1).wrapping_add(PRIME_2);
    let mut v2 = seed.wrapping_add(PRIME_2);
    let mut v3 = seed;
    let mut v4 = seed.wrapping_sub(PRIME_1);

    // process chunks of 32 bytes
    let mut offset_u64_4 = 0;
    let ptr_u64 = data.as_ptr() as *const u64;
    unsafe {
        while offset_u64_4 * CHUNK_SIZE + CHUNK_SIZE <= length_bytes {
            v1 = ingest_one_number(v1, ptr_u64.add(offset_u64_4 * 4).read_unaligned().to_le());
            v2 = ingest_one_number(
                v2,
                ptr_u64.add(offset_u64_4 * 4 + 1).read_unaligned().to_le(),
            );
            v3 = ingest_one_number(
                v3,
                ptr_u64.add(offset_u64_4 * 4 + 2).read_unaligned().to_le(),
            );
            v4 = ingest_one_number(
                v4,
                ptr_u64.add(offset_u64_4 * 4 + 3).read_unaligned().to_le(),
            );
            offset_u64_4 += 1;
        }
    }

    let mut hash = if length_bytes >= CHUNK_SIZE {
        // We have processed at least one full chunk
        let mut hash = v1.rotate_left(1);
        hash = hash.wrapping_add(v2.rotate_left(7));
        hash = hash.wrapping_add(v3.rotate_left(12));
        hash = hash.wrapping_add(v4.rotate_left(18));

        hash = mix_one(hash, v1);
        hash = mix_one(hash, v2);
        hash = mix_one(hash, v3);
        hash = mix_one(hash, v4);

        hash
    } else {
        seed.wrapping_add(PRIME_5)
    };

    hash = hash.wrapping_add(length_bytes as u64);

    // process u64s
    let mut offset_u64 = offset_u64_4 * 4;
    while offset_u64 * 8 + 8 <= length_bytes {
        let mut k1 = unsafe {
            ptr_u64
                .add(offset_u64)
                .read_unaligned()
                .to_le()
                .wrapping_mul(PRIME_2)
        };
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(PRIME_1);
        hash ^= k1;
        hash = hash.rotate_left(27);
        hash = hash.wrapping_mul(PRIME_1);
        hash = hash.wrapping_add(PRIME_4);
        offset_u64 += 1;
    }

    // process u32s
    let data = &data[offset_u64 * 8..];
    let ptr_u32 = data.as_ptr() as *const u32;
    let length_bytes = length_bytes - offset_u64 * 8;
    let mut offset_u32 = 0;
    while offset_u32 * 4 + 4 <= length_bytes {
        let k1 = unsafe {
            u64::from(ptr_u32.add(offset_u32).read_unaligned().to_le()).wrapping_mul(PRIME_1)
        };
        hash ^= k1;
        hash = hash.rotate_left(23);
        hash = hash.wrapping_mul(PRIME_2);
        hash = hash.wrapping_add(PRIME_3);
        offset_u32 += 1;
    }

    // process u8s
    let data = &data[offset_u32 * 4..];
    let length_bytes = length_bytes - offset_u32 * 4;
    let mut offset_u8 = 0;
    while offset_u8 < length_bytes {
        let k1 = u64::from(data[offset_u8]).wrapping_mul(PRIME_5);
        hash ^= k1;
        hash = hash.rotate_left(11);
        hash = hash.wrapping_mul(PRIME_1);
        offset_u8 += 1;
    }

    // The final intermixing
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(PRIME_2);
    hash ^= hash >> 29;
    hash = hash.wrapping_mul(PRIME_3);
    hash ^= hash >> 32;

    hash
}

#[inline(always)]
fn ingest_one_number(mut current_value: u64, mut value: u64) -> u64 {
    value = value.wrapping_mul(PRIME_2);
    current_value = current_value.wrapping_add(value);
    current_value = current_value.rotate_left(31);
    current_value.wrapping_mul(PRIME_1)
}

#[inline(always)]
fn mix_one(mut hash: u64, mut value: u64) -> u64 {
    value = value.wrapping_mul(PRIME_2);
    value = value.rotate_left(31);
    value = value.wrapping_mul(PRIME_1);
    hash ^= value;
    hash = hash.wrapping_mul(PRIME_1);
    hash.wrapping_add(PRIME_4)
}

#[cfg(test)]
mod test {
    use super::spark_compatible_xxhash64;
    use rand::Rng;
    use std::hash::Hasher;
    use twox_hash::XxHash64;

    #[test]
    #[cfg_attr(miri, ignore)] // test takes too long with miri
    fn test_xxhash64_random() {
        let mut rng = rand::thread_rng();
        for len in 0..128 {
            for _ in 0..10 {
                let data: Vec<u8> = (0..len).map(|_| rng.gen()).collect();
                let seed = rng.gen();
                check_xxhash64(&data, seed);
            }
        }
    }

    fn check_xxhash64(data: &[u8], seed: u64) {
        let mut hasher = XxHash64::with_seed(seed);
        hasher.write(data.as_ref());
        let hash1 = hasher.finish();
        let hash2 = spark_compatible_xxhash64(data, seed);
        if hash1 != hash2 {
            panic!("input: {} with seed {seed} produced incorrect hash (comet={hash2}, twox-hash={hash1})",
                   data.iter().fold(String::new(), |mut output, byte| {
                       output.push_str(&format!("{:02x}", byte));
                       output
                   }))
        }
    }
}
