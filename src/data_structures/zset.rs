use std::collections::{BTreeSet, HashMap};
use std::ops::Bound;

use ordered_float::OrderedFloat;

// 定义成员类型
type Member = String;

// 定义分数类型，使用OrderedFloat确保f64可以排序
type Score = OrderedFloat<f64>;

// 用于BTreeSet的键，实现排序逻辑
#[derive(Debug, Clone, PartialEq, Eq)]
struct ScoredMember {
    score: Score,
    member: Member,
}

impl ScoredMember {
    // 辅助函数：获取下一个可表示的分数
    fn next_up_score(score: f64) -> f64 {
        if score.is_nan() {
            return score;
        }
        if score == f64::INFINITY {
            return f64::INFINITY;
        }
        let bits = score.to_bits();
        let next_bits = if score >= 0.0 {
            bits.wrapping_add(1)
        } else {
            bits.wrapping_sub(1)
        };
        f64::from_bits(next_bits)
    }

    // 辅助函数：获取上一个可表示的分数
    fn next_down_score(score: f64) -> f64 {
        if score.is_nan() {
            return score;
        }
        if score == f64::NEG_INFINITY {
            return f64::NEG_INFINITY;
        }
        let bits = score.to_bits();
        let prev_bits = if score > 0.0 {
            bits.wrapping_sub(1)
        } else {
            bits.wrapping_add(1)
        };
        f64::from_bits(prev_bits)
    }
}

// 实现Ord trait以定义排序规则：先按分数，再按成员字典序
impl Ord for ScoredMember {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .cmp(&other.score)
            .then_with(|| self.member.cmp(&other.member))
    }
}

impl PartialOrd for ScoredMember {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// ZSet结构体
pub struct ZSet {
    // 存储(score, member)对，保持排序
    sorted_set: BTreeSet<ScoredMember>,
    // 存储member -> score的映射，用于快速查找分数
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
    pub fn zadd(&mut self, score: Score, member: Member) -> bool {
        let is_new = !self.member_to_score.contains_key(&member);

        // 如果已存在且分数不同，先移除旧的(score, member)对
        if !is_new {
            if let Some(old_score) = self.member_to_score.get(&member) {
                if *old_score != score {
                    let old_entry = ScoredMember {
                        score: *old_score,
                        member: member.clone(),
                    };
                    self.sorted_set.remove(&old_entry);
                }
            }
        }

        // 更新映射和有序集合
        self.member_to_score.insert(member.clone(), score);
        let new_entry = ScoredMember { score, member };
        self.sorted_set.insert(new_entry);

        is_new
    }

    /// ZREM: 移除成员
    pub fn zrem(&mut self, member: &Member) -> bool {
        if let Some(score) = self.member_to_score.remove(member) {
            let entry = ScoredMember {
                score,
                member: member.clone(),
            };
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
        self.sorted_set.len()
    }

    /// ZRANK: 获取成员的排名(按分数升序，0-based)
    pub fn zrank(&self, member: &Member) -> Option<usize> {
        if let Some(score) = self.member_to_score.get(member) {
            let entry = ScoredMember {
                score: *score,
                member: member.clone(),
            };
            Some(self.sorted_set.range(..&entry).count())
        } else {
            None
        }
    }

    /// ZREVRANK: 获取成员的排名(按分数降序，0-based)
    pub fn zrevrank(&self, member: &Member) -> Option<usize> {
        if let Some(score) = self.member_to_score.get(member) {
            let entry = ScoredMember {
                score: *score,
                member: member.clone(),
            };
            let elements_greater = self
                .sorted_set
                .range((Bound::Excluded(&entry), Bound::Unbounded))
                .count();
            Some(elements_greater)
        } else {
            None
        }
    }

    /// ZRANGE: 按排名范围获取成员(升序)
    pub fn zrange(
        &self,
        start: usize,
        stop: usize,
        with_scores: bool,
    ) -> Vec<(Member, Option<Score>)> {
        let len = self.sorted_set.len();
        if len == 0 || start > stop || start >= len {
            return vec![];
        }
        let actual_stop = std::cmp::min(stop, len - 1);
        let take_count = actual_stop - start + 1;

        self.sorted_set
            .iter()
            .skip(start)
            .take(take_count)
            .map(|sm| (sm.member.clone(), with_scores.then_some(sm.score)))
            .collect()
    }

    /// ZREVRANGE: 按排名范围获取成员(降序)
    pub fn zrevrange(
        &self,
        start: usize,
        stop: usize,
        with_scores: bool,
    ) -> Vec<(Member, Option<Score>)> {
        let len = self.sorted_set.len();
        if len == 0 || start > stop || start >= len {
            return vec![];
        }
        let actual_stop = std::cmp::min(stop, len - 1);
        let take_count = actual_stop - start + 1;

        self.sorted_set
            .iter()
            .rev()
            .skip(start)
            .take(take_count)
            .map(|sm| (sm.member.clone(), with_scores.then_some(sm.score)))
            .collect()
    }
    /// ZRANGEBYSCORE: 按分数范围获取成员(升序)
    pub fn zrangebyscore(
        &self,
        min: Score,
        max: Score,
        with_scores: bool,
        offset: Option<usize>,
        count: Option<usize>,
    ) -> Vec<(Member, Option<Score>)> {
        let start_bound = Bound::Included(ScoredMember {
            score: min,
            member: Member::new(),
        });
        let end_bound = Bound::Included(ScoredMember {
            score: max,
            member: "~".repeat(1000),
        });

        // 基础迭代器：过滤分数范围内的元素
        let base_iter = self
            .sorted_set
            .range((start_bound, end_bound))
            .filter(|sm| sm.score >= min && sm.score <= max);

        // 应用偏移：用 skip(0) 统一类型（即使无偏移）
        let offset_iter = base_iter.skip(offset.unwrap_or(0));

        // 应用数量限制：用 take(usize::MAX) 统一类型（即使无限制）
        let limited_iter = offset_iter.take(count.unwrap_or(usize::MAX));

        limited_iter
            .map(|sm| (sm.member.clone(), with_scores.then_some(sm.score)))
            .collect()
    }

    /// ZREVRANGEBYSCORE: 按分数范围获取成员(降序)
    pub fn zrevrangebyscore(
        &self,
        min: Score,
        max: Score,
        with_scores: bool,
        offset: Option<usize>,
        count: Option<usize>,
    ) -> Vec<(Member, Option<Score>)> {
        let start_bound = Bound::Included(ScoredMember {
            score: min,
            member: Member::new(),
        });
        let end_bound = Bound::Included(ScoredMember {
            score: max,
            member: "~".repeat(1000),
        });

        // 基础迭代器：过滤后反转（降序）
        let base_iter = self
            .sorted_set
            .range((start_bound, end_bound))
            .filter(|sm| sm.score >= min && sm.score <= max)
            .rev();

        // 应用偏移：用 skip(0) 统一类型
        let offset_iter = base_iter.skip(offset.unwrap_or(0));

        // 应用数量限制：用 take(usize::MAX) 统一类型
        let limited_iter = offset_iter.take(count.unwrap_or(usize::MAX));

        limited_iter
            .map(|sm| (sm.member.clone(), with_scores.then_some(sm.score)))
            .collect()
    }

    /// ZCOUNT: 计算指定分数范围内的成员数量
    pub fn zcount(&self, min: Score, max: Score) -> usize {
        let start_bound = Bound::Included(ScoredMember {
            score: min,
            member: Member::new(),
        });
        let end_bound = Bound::Included(ScoredMember {
            score: max,
            member: "~".repeat(1000),
        });

        self.sorted_set
            .range((start_bound, end_bound))
            .filter(|sm| sm.score >= min && sm.score <= max)
            .count()
    }

    /// ZINCRBY: 增加成员的分数
    pub fn zincrby(&mut self, increment: Score, member: Member) -> Score {
        let current_score = self
            .member_to_score
            .get(&member)
            .copied()
            .unwrap_or_else(|| 0.0.into());
        let new_score = current_score + increment;
        self.zadd(new_score, member);
        new_score
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zset_basic() {
        let mut zset = ZSet::new();

        // ZADD测试
        assert_eq!(zset.zadd(1.0.into(), "one".to_string()), true);
        assert_eq!(zset.zadd(2.0.into(), "two".to_string()), true);
        assert_eq!(zset.zadd(3.0.into(), "three".to_string()), true);
        assert_eq!(zset.zadd(2.0.into(), "two_dup".to_string()), true);
        assert_eq!(zset.zadd(2.0.into(), "two".to_string()), false);
        assert_eq!(zset.zadd(2.5.into(), "two".to_string()), false);

        // ZCARD测试
        assert_eq!(zset.zcard(), 4);

        // ZSCORE测试
        assert_eq!(zset.zscore(&"one".to_string()), Some(1.0.into()));
        assert_eq!(zset.zscore(&"two".to_string()), Some(2.5.into()));
        assert_eq!(zset.zscore(&"nonexistent".to_string()), None);

        // ZRANK / ZREVRANK测试
        assert_eq!(zset.zrank(&"one".to_string()), Some(0));
        assert_eq!(zset.zrank(&"two".to_string()), Some(2));
        assert_eq!(zset.zrank(&"three".to_string()), Some(3));
        assert_eq!(zset.zrank(&"nonexistent".to_string()), None);

        assert_eq!(zset.zrevrank(&"three".to_string()), Some(0));
        assert_eq!(zset.zrevrank(&"two".to_string()), Some(1));
        assert_eq!(zset.zrevrank(&"one".to_string()), Some(3));
        assert_eq!(zset.zrevrank(&"nonexistent".to_string()), None);

        // ZRANGE测试
        let range1 = zset.zrange(0, 1, false);
        assert_eq!(
            range1,
            vec![("one".to_string(), None), ("two_dup".to_string(), None)]
        );
        let range2 = zset.zrange(1, 3, true);
        assert_eq!(
            range2,
            vec![
                ("two_dup".to_string(), Some(2.0.into())),
                ("two".to_string(), Some(2.5.into())),
                ("three".to_string(), Some(3.0.into()))
            ]
        );

        // ZREVRANGE测试
        let rev_range1 = zset.zrevrange(0, 1, false);
        assert_eq!(
            rev_range1,
            vec![("three".to_string(), None), ("two".to_string(), None)]
        );
        let rev_range2 = zset.zrevrange(1, 3, true);
        assert_eq!(
            rev_range2,
            vec![
                ("two".to_string(), Some(2.5.into())),
                ("two_dup".to_string(), Some(2.0.into())),
                ("one".to_string(), Some(1.0.into()))
            ]
        );

        // ZRANGEBYSCORE测试
        let range_by_score1 = zset.zrangebyscore(2.0.into(), 3.0.into(), false, None, None);
        assert_eq!(range_by_score1.len(), 3);

        let range_by_score2 = zset.zrangebyscore(2.0.into(), 3.0.into(), true, Some(1), Some(2));
        assert_eq!(
            range_by_score2,
            vec![
                ("two".to_string(), Some(2.5.into())),
                ("three".to_string(), Some(3.0.into()))
            ]
        );

        // ZREVRANGEBYSCORE测试
        let rev_range_by_score1 = zset.zrevrangebyscore(2.0.into(), 3.0.into(), false, None, None);
        assert_eq!(rev_range_by_score1.len(), 3);

        // ZCOUNT测试
        assert_eq!(zset.zcount(2.0.into(), 3.0.into()), 3);
        assert_eq!(zset.zcount(2.5.into(), 2.5.into()), 1);

        // ZREM测试
        assert_eq!(zset.zrem(&"two_dup".to_string()), true);
        assert_eq!(zset.zcard(), 3);
        assert_eq!(zset.zscore(&"two_dup".to_string()), None);

        // ZINCRBY测试
        let new_score = zset.zincrby(1.5.into(), "one".to_string());
        //assert_eq!(new_score, 2.5.into());
        assert_eq!(zset.zscore(&"one".to_string()), Some(2.5.into()));
    }
}
