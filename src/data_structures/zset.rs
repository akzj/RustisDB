use std::collections::{BTreeSet, HashMap, HashSet, btree_set};
use std::ops::Bound;

use ordered_float::{FloatCore, OrderedFloat};

// 定义成员类型，这里假设是 String，你可以根据需要改为泛型或其他类型
type Member = String; 

// 定义分数类型，Redis 使用 double (f64)
type Score = OrderedFloat<f64>;

// 用于 BTreeSet 的键，实现排序逻辑
#[derive(Debug, Clone, PartialEq, Eq)]
struct ScoredMember {
    score: Score,
    member: Member,
}

impl ScoredMember {
    // 辅助函数：获取下一个可表示的分数（用于 Excluded 边界）
    // 如果 score 是 f64::MAX，next_up() 通常返回 f64::INFINITY
    // 如果 score 是 f64::INFINITY 或 NaN，行为可能需要特别注意
    fn next_up_score(score: f64) -> f64 {
        if score.is_nan() {
            return score;
        }
        if score == f64::INFINITY {
            return f64::INFINITY;
        }
        // Use bit representation to get the next representable float
        let bits = score.to_bits();
        let next_bits = if score >= 0.0 {
            bits.wrapping_add(1)
        } else {
            bits.wrapping_sub(1)
        };
        f64::from_bits(next_bits)
    }
    
    // 辅助函数：获取上一个可表示的分数（用于 Excluded 边界）
    fn next_down_score(score: f64) -> f64 {
        if score.is_nan() {
            return score;
        }
        if score == f64::NEG_INFINITY {
            return f64::NEG_INFINITY;
        }
        // Use bit representation to get the previous representable float
        let bits = score.to_bits();
        let prev_bits = if score > 0.0 {
            bits.wrapping_sub(1)
        } else {
            bits.wrapping_add(1)
        };
        f64::from_bits(prev_bits)
    }
}

// 实现 Ord trait 以定义排序规则：先按分数，再按成员字典序
impl Ord for ScoredMember {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .partial_cmp(&other.score)
            .unwrap() // f64 的 partial_cmp 在 NaN 时会 panic，这里假设输入是有效分数
            .then_with(|| self.member.cmp(&other.member))
    }
}

impl PartialOrd for ScoredMember {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}


// ZSet 结构体
pub struct ZSet {
    // 存储 (score, member) 对，保持排序
    // 使用 BTreeSet 而非 BTreeMap<(), 因为我们主要关心键的存在和顺序
    sorted_set: BTreeSet<ScoredMember>, 
    // 存储 member -> score 的映射，用于快速查找分数
    member_to_score: HashMap<Member, Score>, 
}

impl ZSet {
    pub fn new() -> Self {
        ZSet {
            sorted_set: BTreeSet::new(),
            member_to_score: HashMap::new(),
        }
    }

    /// ZADD: 添加或更新成员及其分数
    /// 返回是否是新添加的成员 (true: 新增, false: 更新)
    pub fn zadd(&mut self, score: Score, member: Member) -> bool {
        let is_new = !self.member_to_score.contains_key(&member);
        
        // 如果已存在，先移除旧的 (score, member) 对
        if !is_new {
            if let Some(old_score) = self.member_to_score.get(&member) {
                 // 只有当分数真正改变时才需要从 sorted_set 中移除旧项
                 if *old_score != score {
                    let old_entry = ScoredMember { score: *old_score, member: member.clone() };
                    self.sorted_set.remove(&old_entry);
                 }
            }
        }
        
        // 更新 HashMap
        self.member_to_score.insert(member.clone(), score);
        
        // 插入新的 (score, member) 对到 BTreeSet
        let new_entry = ScoredMember { score, member };
        self.sorted_set.insert(new_entry);
        
        is_new
    }

    /// ZREM: 移除成员
    /// 返回是否成功移除 (true: 成功, false: 成员不存在)
    pub fn zrem(&mut self, member: &Member) -> bool {
        if let Some(score) = self.member_to_score.remove(member) {
            let entry = ScoredMember { score, member: member.clone() };
            self.sorted_set.remove(&entry);
            true
        } else {
            false
        }
    }

    /// ZSCORE: 获取成员的分数
    pub fn zscore(&self, member: &Member) -> Option<Score> {
        self.member_to_score.get(member).copied()
    }

    /// ZCARD: 获取集合中成员数量
    pub fn zcard(&self) -> usize {
        self.sorted_set.len() // 或 self.member_to_score.len()
    }

    /// ZRANK: 获取成员的排名 (按分数升序，0-based)
    pub fn zrank(&self, member: &Member) -> Option<usize> {
        if let Some(score) = self.member_to_score.get(member) {
            let entry = ScoredMember { score: *score, member: member.clone() };
            // count for BTreeSet gives the number of elements strictly less than the given key
            Some(self.sorted_set.range(..&entry).count())
        } else {
            None
        }
    }

    /// ZREVRANK: 获取成员的排名 (按分数降序，0-based)
    pub fn zrevrank(&self, member: &Member) -> Option<usize> {
        if let Some(score) = self.member_to_score.get(member) {
            let entry = ScoredMember { score: *score, member: member.clone() };
            // Total elements minus elements less than or equal to entry gives elements greater than entry
            // rank is 0-based, so we subtract 1 from the count of elements > entry to get the rank
            let elements_greater = self.sorted_set.range((Bound::Excluded(&entry), Bound::Unbounded)).count();
            Some(elements_greater)
        } else {
            None
        }
    }

    /// ZRANGE: 按排名范围获取成员 (升序)
    /// start: 起始排名 (inclusive, 0-based)
    /// stop: 结束排名 (inclusive, 0-based)
    /// with_scores: 是否同时返回分数
    pub fn zrange(&self, start: usize, stop: usize, with_scores: bool) -> Vec<(Member, Option<Score>)> {
        let len = self.sorted_set.len();
        if len == 0 || start > stop || start >= len {
            return vec![];
        }
        let actual_stop = std::cmp::min(stop, len - 1);

        self.sorted_set
            .iter()
            .skip(start)
            .take(actual_stop - start + 1)
            .map(|sm| {
                if with_scores {
                    (sm.member.clone(), Some(sm.score))
                } else {
                    (sm.member.clone(), None)
                }
            })
            .collect()
    }

    /// ZREVRANGE: 按排名范围获取成员 (降序)
    /// start: 起始排名 (inclusive, 0-based)
    /// stop: 结束排名 (inclusive, 0-based)
    /// with_scores: 是否同时返回分数
    pub fn zrevrange(&self, start: usize, stop: usize, with_scores: bool) -> Vec<(Member, Option<Score>)> {
        let len = self.sorted_set.len();
        if len == 0 || start > stop || start >= len {
            return vec![];
        }
        let actual_stop = std::cmp::min(stop, len - 1);

        // Collect the relevant slice and then reverse it
        let mut res: Vec<_> = self.sorted_set
            .iter()
            .rev() // Iterate in reverse order
            .skip(start)
            .take(actual_stop - start + 1)
            .map(|sm| {
                if with_scores {
                    (sm.member.clone(), Some(sm.score))
                } else {
                    (sm.member.clone(), None)
                }
            })
            .collect();
         // The collection is already in the correct (reversed) order due to `rev()`
         // res.reverse(); // Not needed if we collect in reverse order directly
         res
    }

    /// ZRANGEBYSCORE: 按分数范围获取成员 (升序)
    /// min: 最小分数 (inclusive)
    /// max: 最大分数 (inclusive)
    /// with_scores: 是否同时返回分数
    /// offset: 跳过的元素数量
    /// count: 返回的元素数量限制
    pub fn zrangebyscore(
        &self,
        min: Score,
        max: Score,
        with_scores: bool,
        offset: Option<usize>,
        count: Option<usize>
    ) -> Vec<(Member, Option<Score>)> {
        let start_bound = Bound::Included(ScoredMember { score: min, member: Member::new() }); // Member::new() is smallest possible string
        // To include max score, we need a key that is >= any member with that score
        // A simple way is to use a member that is lexicographically larger than any possible member.
        // For strings, there isn't a true "max" string, but we can approximate or use a specific large one if domain is known.
        // For simplicity, and because f64::MAX is very large, we might just use MAX score + 1 if needed, 
        // but Bound::Included is fine if we are inclusive on max.
        // Let's assume max is inclusive as per common Redis ZRANGEBYSCORE default.
        // We need to be careful with the upper bound member. 
        // Using a generic max string is tricky. Let's use Excluded with score+epsilon logic or next representable f64,
        // but simpler is to use Included and handle in iteration logic if needed.
        // Actually, let's stick to Included for max, and iterate until score > max.
        
        // More robust way for inclusive max:
        let end_bound = Bound::Included(ScoredMember { score: max, member: "~".repeat(1000) }); // "~" is large in ASCII, repeat to be very large
        
        let mut iter = self.sorted_set.range((start_bound, end_bound));
        
        if let Some(off) = offset {
            iter = iter.skip(off);
        }
        if let Some(cnt) = count {
            iter = iter.take(cnt);
        }

        iter.filter(|sm| sm.score >= min && sm.score <= max) // Double-check score bounds due to member part of key
            .map(|sm| {
                if with_scores {
                    (sm.member.clone(), Some(sm.score))
                } else {
                    (sm.member.clone(), None)
                }
            })
            .collect()
    }

    /// ZREVRANGEBYSCORE: 按分数范围获取成员 (降序)
    /// min: 最小分数 (inclusive)
    /// max: 最大分数 (inclusive)
    /// with_scores: 是否同时返回分数
    /// offset: 跳过的元素数量
    /// count: 返回的元素数量限制
    pub fn zrevrangebyscore(
        &self,
        min: Score,
        max: Score,
        with_scores: bool,
        offset: Option<usize>,
        count: Option<usize>
    ) -> Vec<(Member, Option<Score>)> {
        // Similar to zrangebyscore but iterate in reverse order
        let start_bound = Bound::Included(ScoredMember { score: min, member: Member::new() });
        let end_bound = Bound::Included(ScoredMember { score: max, member: "~".repeat(1000) });
        
        let mut iter = self.sorted_set.range((start_bound, end_bound)).rev(); // Note .rev() here
        
        if let Some(off) = offset {
            iter = iter.skip(off);
        }
        if let Some(cnt) = count {
            iter = iter.take(cnt);
        }

        iter.filter(|sm| sm.score >= min && sm.score <= max)
            .map(|sm| {
                if with_scores {
                    (sm.member.clone(), Some(sm.score))
                } else {
                    (sm.member.clone(), None)
                }
            })
            .collect()
    }

    
    /// ZCOUNT: 计算指定分数范围内的成员数量
    pub fn zcount(&self, min: Score, max: Score) -> usize {
        let start_bound = Bound::Included(ScoredMember { score: min, member: Member::new() });
        let end_bound = Bound::Included(ScoredMember { score: max, member: "~".repeat(1000) });
        
        self.sorted_set.range((start_bound, end_bound)).filter(|sm| sm.score >= min && sm.score <= max).count()
    }

    /// ZINCRBY: 增加成员的分数
    pub fn zincrby(&mut self, increment: Score, member: Member) -> Score {
        let new_score = self.member_to_score.get(&member).copied().unwrap_or(0.0) + increment;
        self.zadd(new_score, member); // zadd handles update logic
        new_score
    }
    
    // 可以添加更多方法，如 ZREMRangeByRank, ZREMRangeByScore 等
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zset_basic() {
        let mut zset = ZSet::new();

        // ZADD
        assert_eq!(zset.zadd(1.0, "one".to_string()), true);
        assert_eq!(zset.zadd(2.0, "two".to_string()), true);
        assert_eq!(zset.zadd(3.0, "three".to_string()), true);
        assert_eq!(zset.zadd(2.0, "two_dup".to_string()), true); // Same score, different member
        assert_eq!(zset.zadd(2.0, "two".to_string()), false); // Update existing member's score (same score)
        assert_eq!(zset.zadd(2.5, "two".to_string()), false); // Update existing member's score (different score)

        // ZCARD
        assert_eq!(zset.zcard(), 4);

        // ZSCORE
        assert_eq!(zset.zscore(&"one".to_string()), Some(1.0));
        assert_eq!(zset.zscore(&"two".to_string()), Some(2.5)); // Updated score
        assert_eq!(zset.zscore(&"nonexistent".to_string()), None);

        // ZRANK / ZREVRANK
        assert_eq!(zset.zrank(&"one".to_string()), Some(0));
        assert_eq!(zset.zrank(&"two".to_string()), Some(2)); // Rank after "two_dup" (score 2.0) and "one" (score 1.0)
        assert_eq!(zset.zrank(&"three".to_string()), Some(3));
        assert_eq!(zset.zrank(&"nonexistent".to_string()), None);
        
        assert_eq!(zset.zrevrank(&"three".to_string()), Some(0)); // Highest score
        assert_eq!(zset.zrevrank(&"two".to_string()), Some(1)); // Second highest
        assert_eq!(zset.zrevrank(&"one".to_string()), Some(3)); // Lowest score
        assert_eq!(zset.zrevrank(&"nonexistent".to_string()), None);

        // ZRANGE
        let range1 = zset.zrange(0, 1, false);
        assert_eq!(range1, vec![("one".to_string(), None), ("two_dup".to_string(), None)]);
        let range2 = zset.zrange(1, 3, true);
        assert_eq!(range2, vec![("two_dup".to_string(), Some(2.0)), ("two".to_string(), Some(2.5)), ("three".to_string(), Some(3.0))]);

        // ZREVRANGE
        let rev_range1 = zset.zrevrange(0, 1, false);
        assert_eq!(rev_range1, vec![("three".to_string(), None), ("two".to_string(), None)]);
        let rev_range2 = zset.zrevrange(1, 3, true);
        assert_eq!(rev_range2, vec![("two".to_string(), Some(2.5)), ("two_dup".to_string(), Some(2.0)), ("one".to_string(), Some(1.0))]);

        // ZRANGEBYSCORE
        let range_by_score1 = zset.zrangebyscore(2.0, 3.0, false, None, None);
        // Should include "two_dup" (2.0), "two" (2.5), "three" (3.0)
        assert_eq!(range_by_score1.len(), 3);
        assert!(range_by_score1.contains(&("two_dup".to_string(), None)));
        assert!(range_by_score1.contains(&("two".to_string(), None)));
        assert!(range_by_score1.contains(&("three".to_string(), None)));

        let range_by_score2 = zset.zrangebyscore(2.0, 3.0, true, Some(1), Some(2)); // Offset 1, Count 2
        // Should be "two" (2.5) and "three" (3.0)
        assert_eq!(range_by_score2, vec![("two".to_string(), Some(2.5)), ("three".to_string(), Some(3.0))]);

        // ZREVRANGEBYSCORE
        let rev_range_by_score1 = zset.zrevrangebyscore(2.0, 3.0, false, None, None);
        // Should include "three" (3.0), "two" (2.5), "two_dup" (2.0)
        assert_eq!(rev_range_by_score1.len(), 3);
        assert!(rev_range_by_score1.first() == Some(&("three".to_string(), None)));
        assert!(rev_range_by_score1.last() == Some(&("two_dup".to_string(), None)));

        // ZCOUNT
        assert_eq!(zset.zcount(2.0, 3.0), 3);
        assert_eq!(zset.zcount(2.5, 2.5), 1); // Only "two"
        
        // ZREM
        assert_eq!(zset.zrem(&"two_dup".to_string()), true);
        assert_eq!(zset.zcard(), 3);
        assert_eq!(zset.zscore(&"two_dup".to_string()), None);
        assert_eq!(zset.zrem(&"nonexistent".to_string()), false);
        
        // ZINCRBY
        let new_score = zset.zincrby(1.5, "one".to_string());
        assert_eq!(new_score, 2.5); // 1.0 + 1.5
        assert_eq!(zset.zscore(&"one".to_string()), Some(2.5));
        // "one" score is now 2.5, same as "two". "one" < "two" lexicographically.
        assert_eq!(zset.zrank(&"one".to_string()), Some(0)); // "one" (2.5) comes before "two" (2.5) due to lex
        assert_eq!(zset.zrank(&"two".to_string()), Some(1)); 
    }
}