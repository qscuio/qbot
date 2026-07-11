use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::mentions::{ClusterMention, EventMention};

#[derive(Debug, Clone, PartialEq)]
pub struct ClusterDecision {
    pub event_cluster_id: Uuid,
    pub confidence: f64,
    pub reason_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CandidateCluster {
    pub event_cluster_id: Uuid,
    pub cluster_version: Option<i32>,
    pub input_cluster_versions: Vec<ClusterVersionRef>,
    pub mentions: Vec<ClusterMention>,
    pub review_required: bool,
}

impl CandidateCluster {
    pub fn independent_sources(&self) -> usize {
        independent_source_count(&self.mentions)
    }

    pub fn source_entropy(&self) -> f64 {
        source_entropy(&self.mentions)
    }

    pub fn representative_evidence_id(&self) -> Uuid {
        representative_mention(&self.mentions)
            .map(|mention| mention.mention.evidence_id)
            .unwrap_or_else(Uuid::nil)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ClusterVersionRef {
    pub event_cluster_id: Uuid,
    pub cluster_version: Option<i32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RefinedCluster {
    pub event_cluster_id: Uuid,
    pub cluster_version: i32,
    pub supersedes_version: Option<i32>,
    pub input_cluster_versions: Vec<ClusterVersionRef>,
    pub mentions: Vec<ClusterMention>,
    pub representative_evidence_id: Uuid,
    pub independent_sources: usize,
    pub source_entropy: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum IncrementalAssignment {
    NewCluster { event_cluster_id: Uuid },
    AutoJoin(ClusterDecision),
    ReviewRequired(ClusterDecision),
}

#[derive(Debug, Clone, PartialEq)]
pub struct IncrementalClusteringConfig {
    pub auto_join_threshold: f64,
    pub review_threshold: f64,
    pub max_time_distance_hours: i64,
}

impl Default for IncrementalClusteringConfig {
    fn default() -> Self {
        Self {
            auto_join_threshold: 0.82,
            review_threshold: 0.55,
            max_time_distance_hours: 48,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LockedClusterRelations {
    pub merge_pairs: BTreeSet<(Uuid, Uuid)>,
    pub split_pairs: BTreeSet<(Uuid, Uuid)>,
}

#[derive(Debug, Clone)]
pub struct IncrementalClusterer {
    config: IncrementalClusteringConfig,
    clusters: Vec<CandidateCluster>,
}

impl IncrementalClusterer {
    pub fn new(config: IncrementalClusteringConfig) -> Self {
        Self {
            config,
            clusters: Vec::new(),
        }
    }

    pub fn clusters(&self) -> &[CandidateCluster] {
        &self.clusters
    }

    pub fn ingest_mention(
        &mut self,
        mention: EventMention,
        duplicate_group_id: Option<Uuid>,
    ) -> IncrementalAssignment {
        let best_match = self
            .clusters
            .iter()
            .enumerate()
            .filter_map(|(index, cluster)| {
                score_against_cluster(&mention, cluster, &self.config).map(|score| (index, score))
            })
            .max_by(|left, right| compare_cluster_scores(&left.1, &right.1));

        if let Some((index, score)) = best_match {
            if score.score >= self.config.auto_join_threshold {
                let cluster = &mut self.clusters[index];
                cluster.mentions.push(ClusterMention {
                    origin_cluster_id: cluster.event_cluster_id,
                    duplicate_group_id,
                    mention,
                });
                return IncrementalAssignment::AutoJoin(ClusterDecision {
                    event_cluster_id: cluster.event_cluster_id,
                    confidence: score.score,
                    reason_codes: decision_reason_codes(&score, "auto_join"),
                });
            }

            if score.score >= self.config.review_threshold {
                let cluster = &mut self.clusters[index];
                cluster.review_required = true;
                cluster.mentions.push(ClusterMention {
                    origin_cluster_id: cluster.event_cluster_id,
                    duplicate_group_id,
                    mention,
                });
                return IncrementalAssignment::ReviewRequired(ClusterDecision {
                    event_cluster_id: cluster.event_cluster_id,
                    confidence: score.score,
                    reason_codes: decision_reason_codes(&score, "review_required"),
                });
            }
        }

        let event_cluster_id = Uuid::new_v4();
        self.clusters.push(CandidateCluster {
            event_cluster_id,
            cluster_version: None,
            input_cluster_versions: vec![ClusterVersionRef {
                event_cluster_id,
                cluster_version: None,
            }],
            mentions: vec![ClusterMention {
                origin_cluster_id: event_cluster_id,
                duplicate_group_id,
                mention,
            }],
            review_required: false,
        });
        IncrementalAssignment::NewCluster { event_cluster_id }
    }
}

#[derive(Debug, Clone)]
pub struct EndOfDayRefiner {
    config: IncrementalClusteringConfig,
}

impl EndOfDayRefiner {
    pub fn new(config: IncrementalClusteringConfig) -> Self {
        Self { config }
    }

    pub fn refine(
        &self,
        clusters: &[CandidateCluster],
        locked_relations: &LockedClusterRelations,
    ) -> Vec<RefinedCluster> {
        let merged_groups = merge_fragmented_clusters(clusters, locked_relations, &self.config);
        merged_groups
            .iter()
            .flat_map(|group| split_overbroad_cluster(group, locked_relations, &self.config))
            .collect()
    }
}

#[derive(Debug, Clone)]
struct ClusterScore {
    event_cluster_id: Uuid,
    score: f64,
    hard_conditions_met: bool,
    components: SimilarityComponents,
}

#[derive(Debug, Clone, Copy)]
struct SimilarityComponents {
    time_proximity: f64,
    entity_overlap: f64,
    action_overlap: f64,
    location_overlap: f64,
    semantic_similarity: f64,
}

impl SimilarityComponents {
    fn weighted_score(&self) -> f64 {
        ((self.time_proximity * 0.2)
            + (self.entity_overlap * 0.25)
            + (self.action_overlap * 0.2)
            + (self.location_overlap * 0.1)
            + (self.semantic_similarity * 0.25))
            .clamp(0.0, 1.0)
    }
}

fn compare_cluster_scores(left: &ClusterScore, right: &ClusterScore) -> Ordering {
    left.score
        .total_cmp(&right.score)
        .then_with(|| right.event_cluster_id.cmp(&left.event_cluster_id))
}

fn decision_reason_codes(score: &ClusterScore, terminal_reason: &str) -> Vec<String> {
    let mut reason_codes = Vec::new();
    if score.components.time_proximity > 0.0 {
        reason_codes.push("time_proximity".to_string());
    }
    if score.components.entity_overlap > 0.0 {
        reason_codes.push("entity_overlap".to_string());
    }
    if score.components.action_overlap > 0.0 {
        reason_codes.push("action_overlap".to_string());
    }
    if score.components.location_overlap > 0.0 {
        reason_codes.push("location_overlap".to_string());
    }
    if score.components.semantic_similarity > 0.0 {
        reason_codes.push("semantic_similarity".to_string());
    }
    if score.hard_conditions_met {
        reason_codes.push("hard_conditions_met".to_string());
    }
    reason_codes.push(terminal_reason.to_string());
    reason_codes
}

fn score_against_cluster(
    mention: &EventMention,
    cluster: &CandidateCluster,
    config: &IncrementalClusteringConfig,
) -> Option<ClusterScore> {
    cluster
        .mentions
        .iter()
        .map(|existing| score_mentions(mention, &existing.mention, config))
        .filter(|score| score.hard_conditions_met)
        .max_by(compare_cluster_scores)
        .map(|mut score| {
            score.event_cluster_id = cluster.event_cluster_id;
            score
        })
}

fn score_mentions(
    left: &EventMention,
    right: &EventMention,
    config: &IncrementalClusteringConfig,
) -> ClusterScore {
    let (time_proximity, time_is_compatible) = time_proximity(
        left.event_time,
        right.event_time,
        config.max_time_distance_hours,
    );
    let entity_overlap = token_overlap(&left.entity_ids, &right.entity_ids);
    let action_overlap = token_overlap(&left.action_tokens, &right.action_tokens);
    let location_overlap = location_overlap(&left.location_tokens, &right.location_tokens);
    let semantic_similarity = semantic_similarity(&left.semantic_vector, &right.semantic_vector);
    let entity_intersection_non_empty =
        has_non_empty_token_intersection(&left.entity_ids, &right.entity_ids);
    let action_intersection_non_empty =
        has_non_empty_token_intersection(&left.action_tokens, &right.action_tokens);
    let components = SimilarityComponents {
        time_proximity,
        entity_overlap,
        action_overlap,
        location_overlap,
        semantic_similarity,
    };
    let hard_conditions_met =
        time_is_compatible && entity_intersection_non_empty && action_intersection_non_empty;

    ClusterScore {
        event_cluster_id: Uuid::nil(),
        score: components.weighted_score(),
        hard_conditions_met,
        components,
    }
}

fn merge_fragmented_clusters(
    clusters: &[CandidateCluster],
    locked_relations: &LockedClusterRelations,
    config: &IncrementalClusteringConfig,
) -> Vec<CandidateCluster> {
    if clusters.is_empty() {
        return Vec::new();
    }

    let mut parents = (0..clusters.len()).collect::<Vec<_>>();

    for left in 0..clusters.len() {
        for right in (left + 1)..clusters.len() {
            if should_merge_clusters(&clusters[left], &clusters[right], locked_relations, config) {
                union_indices(&mut parents, left, right);
            }
        }
    }

    let mut grouped = BTreeMap::<usize, CandidateCluster>::new();
    for (index, cluster) in clusters.iter().enumerate() {
        let root = find_index(&mut parents, index);
        let entry = grouped.entry(root).or_insert_with(|| CandidateCluster {
            event_cluster_id: cluster.event_cluster_id,
            cluster_version: cluster.cluster_version,
            input_cluster_versions: cluster.input_cluster_versions.clone(),
            mentions: Vec::new(),
            review_required: false,
        });
        entry.input_cluster_versions = merged_cluster_version_refs(
            &entry.input_cluster_versions,
            &cluster.input_cluster_versions,
        );
        entry.review_required |= cluster.review_required;
        entry.mentions.extend(cluster.mentions.iter().cloned());
        entry.event_cluster_id = preferred_cluster_id(
            cluster_origin_ids(entry),
            &entry.input_cluster_versions,
            entry.event_cluster_id,
        );
        entry.cluster_version =
            cluster_version_for_id(&entry.input_cluster_versions, entry.event_cluster_id);
    }

    grouped.into_values().collect()
}

fn should_merge_clusters(
    left: &CandidateCluster,
    right: &CandidateCluster,
    locked_relations: &LockedClusterRelations,
    config: &IncrementalClusteringConfig,
) -> bool {
    if relation_blocks_merge(
        cluster_origin_ids(left),
        cluster_origin_ids(right),
        &locked_relations.split_pairs,
    ) {
        return false;
    }

    if relation_forces_merge(
        cluster_origin_ids(left),
        cluster_origin_ids(right),
        &locked_relations.merge_pairs,
    ) {
        return true;
    }

    best_cross_cluster_score(left, right, config)
        .map(|score| score.hard_conditions_met && score.score >= config.auto_join_threshold)
        .unwrap_or(false)
}

fn best_cross_cluster_score(
    left: &CandidateCluster,
    right: &CandidateCluster,
    config: &IncrementalClusteringConfig,
) -> Option<ClusterScore> {
    left.mentions
        .iter()
        .flat_map(|left_mention| {
            right.mentions.iter().map(|right_mention| {
                score_mentions(&left_mention.mention, &right_mention.mention, config)
            })
        })
        .max_by(compare_cluster_scores)
}

fn split_overbroad_cluster(
    cluster: &CandidateCluster,
    locked_relations: &LockedClusterRelations,
    config: &IncrementalClusteringConfig,
) -> Vec<RefinedCluster> {
    let components = connected_components(cluster, locked_relations, config);
    let mut used_ids = BTreeSet::new();

    components
        .into_iter()
        .enumerate()
        .map(|(index, mentions)| {
            let input_cluster_versions =
                input_cluster_versions_for_mentions(&mentions, &cluster.input_cluster_versions);
            let preferred_id = preferred_cluster_id(
                mentions
                    .iter()
                    .map(|mention| mention.origin_cluster_id)
                    .collect(),
                &input_cluster_versions,
                cluster.event_cluster_id,
            );
            let event_cluster_id = if used_ids.insert(preferred_id) {
                preferred_id
            } else {
                derived_split_cluster_id(preferred_id, index, mentions[0].mention.mention_id)
            };
            let supersedes_version =
                cluster_version_for_id(&input_cluster_versions, event_cluster_id);
            let cluster_version = supersedes_version.map_or(1, |version| version + 1);
            let representative_evidence_id = representative_mention(&mentions)
                .map(|mention| mention.mention.evidence_id)
                .unwrap_or_else(Uuid::nil);

            RefinedCluster {
                event_cluster_id,
                cluster_version,
                supersedes_version,
                input_cluster_versions,
                representative_evidence_id,
                independent_sources: independent_source_count(&mentions),
                source_entropy: source_entropy(&mentions),
                mentions,
            }
        })
        .collect()
}

fn connected_components(
    cluster: &CandidateCluster,
    locked_relations: &LockedClusterRelations,
    config: &IncrementalClusteringConfig,
) -> Vec<Vec<ClusterMention>> {
    let mut visited = vec![false; cluster.mentions.len()];
    let mut components = Vec::new();

    for start in 0..cluster.mentions.len() {
        if visited[start] {
            continue;
        }

        let mut stack = vec![start];
        let mut component_indices = Vec::new();
        visited[start] = true;

        while let Some(index) = stack.pop() {
            component_indices.push(index);
            for candidate in 0..cluster.mentions.len() {
                if visited[candidate] || index == candidate {
                    continue;
                }
                if mentions_are_connected(
                    &cluster.mentions[index],
                    &cluster.mentions[candidate],
                    locked_relations,
                    config,
                ) {
                    visited[candidate] = true;
                    stack.push(candidate);
                }
            }
        }

        let mut component = component_indices
            .into_iter()
            .map(|index| cluster.mentions[index].clone())
            .collect::<Vec<_>>();
        component.sort_by(compare_cluster_mentions);
        components.push(component);
    }

    components.sort_by(|left, right| compare_components(left, right));
    components
}

fn mentions_are_connected(
    left: &ClusterMention,
    right: &ClusterMention,
    locked_relations: &LockedClusterRelations,
    config: &IncrementalClusteringConfig,
) -> bool {
    let origin_pair = ordered_pair(left.origin_cluster_id, right.origin_cluster_id);
    if locked_relations.split_pairs.contains(&origin_pair) {
        return false;
    }
    if locked_relations.merge_pairs.contains(&origin_pair) {
        return true;
    }

    let score = score_mentions(&left.mention, &right.mention, config);
    score.hard_conditions_met && score.score >= config.review_threshold
}

fn relation_blocks_merge(
    left_origins: BTreeSet<Uuid>,
    right_origins: BTreeSet<Uuid>,
    split_pairs: &BTreeSet<(Uuid, Uuid)>,
) -> bool {
    left_origins.iter().any(|left| {
        right_origins
            .iter()
            .any(|right| split_pairs.contains(&ordered_pair(*left, *right)))
    })
}

fn relation_forces_merge(
    left_origins: BTreeSet<Uuid>,
    right_origins: BTreeSet<Uuid>,
    merge_pairs: &BTreeSet<(Uuid, Uuid)>,
) -> bool {
    left_origins.iter().any(|left| {
        right_origins
            .iter()
            .any(|right| merge_pairs.contains(&ordered_pair(*left, *right)))
    })
}

fn cluster_origin_ids(cluster: &CandidateCluster) -> BTreeSet<Uuid> {
    cluster
        .mentions
        .iter()
        .map(|mention| mention.origin_cluster_id)
        .collect()
}

fn preferred_cluster_id(
    origin_ids: BTreeSet<Uuid>,
    input_cluster_versions: &[ClusterVersionRef],
    fallback: Uuid,
) -> Uuid {
    input_cluster_versions
        .iter()
        .filter(|cluster_ref| {
            cluster_ref.cluster_version.is_some()
                && origin_ids.contains(&cluster_ref.event_cluster_id)
        })
        .map(|cluster_ref| cluster_ref.event_cluster_id)
        .min()
        .or_else(|| origin_ids.iter().copied().min())
        .unwrap_or(fallback)
}

fn cluster_version_for_id(
    input_cluster_versions: &[ClusterVersionRef],
    event_cluster_id: Uuid,
) -> Option<i32> {
    input_cluster_versions
        .iter()
        .find(|cluster_ref| cluster_ref.event_cluster_id == event_cluster_id)
        .and_then(|cluster_ref| cluster_ref.cluster_version)
}

fn input_cluster_versions_for_mentions(
    mentions: &[ClusterMention],
    input_cluster_versions: &[ClusterVersionRef],
) -> Vec<ClusterVersionRef> {
    let component_origin_ids = mentions
        .iter()
        .map(|mention| mention.origin_cluster_id)
        .collect::<BTreeSet<_>>();
    input_cluster_versions
        .iter()
        .filter(|cluster_ref| component_origin_ids.contains(&cluster_ref.event_cluster_id))
        .cloned()
        .collect()
}

fn merged_cluster_version_refs(
    left: &[ClusterVersionRef],
    right: &[ClusterVersionRef],
) -> Vec<ClusterVersionRef> {
    let mut merged = BTreeMap::new();
    for cluster_ref in left.iter().chain(right.iter()) {
        merged
            .entry(cluster_ref.event_cluster_id)
            .and_modify(|current| {
                if compare_cluster_versions(cluster_ref.cluster_version, *current)
                    == Ordering::Greater
                {
                    *current = cluster_ref.cluster_version;
                }
            })
            .or_insert(cluster_ref.cluster_version);
    }

    merged
        .into_iter()
        .map(|(event_cluster_id, cluster_version)| ClusterVersionRef {
            event_cluster_id,
            cluster_version,
        })
        .collect()
}

fn compare_cluster_versions(left: Option<i32>, right: Option<i32>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => left.cmp(&right),
        (Some(_), None) => Ordering::Greater,
        (None, Some(_)) => Ordering::Less,
        (None, None) => Ordering::Equal,
    }
}

fn find_index(parents: &mut [usize], index: usize) -> usize {
    if parents[index] != index {
        let root = find_index(parents, parents[index]);
        parents[index] = root;
    }
    parents[index]
}

fn union_indices(parents: &mut [usize], left: usize, right: usize) {
    let left_root = find_index(parents, left);
    let right_root = find_index(parents, right);
    if left_root != right_root {
        let (winner, loser) = if left_root < right_root {
            (left_root, right_root)
        } else {
            (right_root, left_root)
        };
        parents[loser] = winner;
    }
}

fn independent_source_count(mentions: &[ClusterMention]) -> usize {
    source_units(mentions).len()
}

fn source_entropy(mentions: &[ClusterMention]) -> f64 {
    let weights = source_units(mentions)
        .into_values()
        .map(|weight| {
            if weight.is_finite() && weight > 0.0 {
                weight
            } else {
                1.0
            }
        })
        .collect::<Vec<_>>();
    if weights.len() <= 1 {
        return 0.0;
    }

    let total = weights.iter().sum::<f64>();
    if total <= 0.0 {
        return 0.0;
    }

    weights
        .iter()
        .map(|weight| {
            let probability = weight / total;
            -(probability * probability.log2())
        })
        .sum()
}

fn source_units(mentions: &[ClusterMention]) -> BTreeMap<Uuid, f64> {
    let mut units = BTreeMap::new();
    for mention in mentions {
        let unit_id = mention
            .duplicate_group_id
            .unwrap_or(mention.mention.mention_id);
        let weight = mention.mention.source_independence;
        units
            .entry(unit_id)
            .and_modify(|current| {
                if weight.total_cmp(current) == Ordering::Greater {
                    *current = weight;
                }
            })
            .or_insert(weight);
    }
    units
}

fn representative_mention(mentions: &[ClusterMention]) -> Option<&ClusterMention> {
    mentions
        .iter()
        .max_by(|left, right| compare_cluster_mentions(left, right))
}

fn compare_cluster_mentions(left: &ClusterMention, right: &ClusterMention) -> Ordering {
    left.mention
        .adds_new_fact
        .cmp(&right.mention.adds_new_fact)
        .then_with(|| {
            normalized_independence(left.mention.source_independence)
                .total_cmp(&normalized_independence(right.mention.source_independence))
        })
        .then_with(|| compare_event_time(left.mention.event_time, right.mention.event_time))
        .then_with(|| right.mention.mention_id.cmp(&left.mention.mention_id))
}

fn compare_components(left: &[ClusterMention], right: &[ClusterMention]) -> Ordering {
    let left_representative = representative_mention(left).expect("component must not be empty");
    let right_representative = representative_mention(right).expect("component must not be empty");
    compare_cluster_mentions(left_representative, right_representative)
        .reverse()
        .then_with(|| left.len().cmp(&right.len()))
}

fn compare_event_time(left: Option<DateTime<Utc>>, right: Option<DateTime<Utc>>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => right.cmp(&left),
        (Some(_), None) => Ordering::Greater,
        (None, Some(_)) => Ordering::Less,
        (None, None) => Ordering::Equal,
    }
}

fn normalized_independence(value: f64) -> f64 {
    if value.is_finite() {
        value
    } else {
        0.0
    }
}

fn time_proximity(
    left: Option<DateTime<Utc>>,
    right: Option<DateTime<Utc>>,
    max_time_distance_hours: i64,
) -> (f64, bool) {
    match (left, right) {
        (Some(left), Some(right)) => {
            let distance_seconds = (left - right).num_seconds().abs() as f64;
            let max_distance_seconds = (max_time_distance_hours.max(1) as f64) * 3600.0;
            if distance_seconds > max_distance_seconds {
                (0.0, false)
            } else {
                (1.0 - (distance_seconds / max_distance_seconds), true)
            }
        }
        _ => (0.5, true),
    }
}

fn token_overlap(left: &[String], right: &[String]) -> f64 {
    let left_tokens = normalized_token_set(left);
    let right_tokens = normalized_token_set(right);
    if left_tokens.is_empty() && right_tokens.is_empty() {
        return 1.0;
    }
    if left_tokens.is_empty() || right_tokens.is_empty() {
        return 0.0;
    }

    let intersection = left_tokens.intersection(&right_tokens).count();
    let union = left_tokens.union(&right_tokens).count();
    intersection as f64 / union as f64
}

fn has_non_empty_token_intersection(left: &[String], right: &[String]) -> bool {
    let left_tokens = normalized_token_set(left);
    let right_tokens = normalized_token_set(right);
    !left_tokens.is_empty()
        && !right_tokens.is_empty()
        && left_tokens.intersection(&right_tokens).next().is_some()
}

fn location_overlap(left: &[String], right: &[String]) -> f64 {
    if left.is_empty() && right.is_empty() {
        return 1.0;
    }
    if left.is_empty() || right.is_empty() {
        return 0.5;
    }
    token_overlap(left, right)
}

fn normalized_token_set(tokens: &[String]) -> BTreeSet<String> {
    tokens.iter().map(|token| token.to_lowercase()).collect()
}

fn semantic_similarity(left: &[f32], right: &[f32]) -> f64 {
    if left.is_empty() || right.is_empty() || left.len() != right.len() {
        return 0.0;
    }

    let dot_product = left
        .iter()
        .zip(right.iter())
        .map(|(left, right)| (*left as f64) * (*right as f64))
        .sum::<f64>();
    let left_norm = left
        .iter()
        .map(|value| (*value as f64) * (*value as f64))
        .sum::<f64>()
        .sqrt();
    let right_norm = right
        .iter()
        .map(|value| (*value as f64) * (*value as f64))
        .sum::<f64>()
        .sqrt();

    if left_norm == 0.0 || right_norm == 0.0 {
        0.0
    } else {
        (dot_product / (left_norm * right_norm)).clamp(0.0, 1.0)
    }
}

fn ordered_pair(left: Uuid, right: Uuid) -> (Uuid, Uuid) {
    if left <= right {
        (left, right)
    } else {
        (right, left)
    }
}

fn derived_split_cluster_id(
    origin_cluster_id: Uuid,
    index: usize,
    leader_mention_id: Uuid,
) -> Uuid {
    let digest = Sha256::digest(
        format!("event-cluster-split:{origin_cluster_id}:{index}:{leader_mention_id}").as_bytes(),
    );
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use uuid::Uuid;

    use super::{
        semantic_similarity, CandidateCluster, ClusterVersionRef, EndOfDayRefiner,
        IncrementalAssignment, IncrementalClusterer, IncrementalClusteringConfig,
        LockedClusterRelations,
    };
    use crate::analysis::events::mentions::{ClusterMention, EventMention};

    #[test]
    fn same_entities_action_date_and_high_similarity_auto_join() {
        let mut clusterer = IncrementalClusterer::new(IncrementalClusteringConfig::default());
        let seed = mention(
            "ACME contract award",
            Some(dt(2026, 7, 10, 8)),
            &["acme"],
            &["award", "contract"],
            &["shanghai"],
            true,
            0.95,
        );
        let follow_up = mention(
            "ACME wins contract award",
            Some(dt(2026, 7, 10, 10)),
            &["acme"],
            &["award", "contract"],
            &["shanghai"],
            true,
            0.88,
        );

        let first = clusterer.ingest_mention(seed, None);
        let second = clusterer.ingest_mention(follow_up, None);

        let cluster_id = match first {
            IncrementalAssignment::NewCluster { event_cluster_id } => event_cluster_id,
            other => panic!("expected a new cluster for the seed mention, got {other:?}"),
        };
        let decision = match second {
            IncrementalAssignment::AutoJoin(decision) => decision,
            other => panic!("expected an auto-join decision, got {other:?}"),
        };

        assert_eq!(decision.event_cluster_id, cluster_id);
        assert!(decision.confidence >= IncrementalClusteringConfig::default().auto_join_threshold);
        assert!(decision.reason_codes.iter().any(|code| code == "auto_join"));
        assert_eq!(clusterer.clusters().len(), 1);
        assert_eq!(clusterer.clusters()[0].mentions.len(), 2);
    }

    #[test]
    fn same_company_but_different_action_does_not_auto_join() {
        let mut clusterer = IncrementalClusterer::new(IncrementalClusteringConfig::default());

        clusterer.ingest_mention(
            mention(
                "ACME launches share buyback",
                Some(dt(2026, 7, 10, 8)),
                &["acme"],
                &["launch", "buyback"],
                &["shenzhen"],
                true,
                0.92,
            ),
            None,
        );
        let outcome = clusterer.ingest_mention(
            mention(
                "ACME appoints new chief executive",
                Some(dt(2026, 7, 10, 9)),
                &["acme"],
                &["appoint", "executive"],
                &["shenzhen"],
                true,
                0.91,
            ),
            None,
        );

        assert!(matches!(outcome, IncrementalAssignment::NewCluster { .. }));
        assert_eq!(clusterer.clusters().len(), 2);
    }

    #[test]
    fn empty_entity_and_action_sets_do_not_satisfy_auto_join_hard_conditions() {
        let mut clusterer = IncrementalClusterer::new(IncrementalClusteringConfig::default());

        clusterer.ingest_mention(
            mention(
                "Market chatter points to a major update",
                Some(dt(2026, 7, 10, 8)),
                &[],
                &[],
                &["shanghai"],
                true,
                0.92,
            ),
            None,
        );
        let outcome = clusterer.ingest_mention(
            mention(
                "Market chatter points to a major update",
                Some(dt(2026, 7, 10, 9)),
                &[],
                &[],
                &["shanghai"],
                false,
                0.81,
            ),
            None,
        );

        assert!(matches!(outcome, IncrementalAssignment::NewCluster { .. }));
        assert_eq!(clusterer.clusters().len(), 2);
    }

    #[test]
    fn low_confidence_candidate_becomes_review_required() {
        let mut clusterer = IncrementalClusterer::new(IncrementalClusteringConfig::default());
        clusterer.ingest_mention(
            mention(
                "ACME signs logistics memorandum",
                Some(dt(2026, 7, 10, 8)),
                &["acme", "logisticsco"],
                &["sign", "memorandum"],
                &["ningbo"],
                true,
                0.93,
            ),
            None,
        );

        let outcome = clusterer.ingest_mention(
            mention(
                "ACME memorandum update",
                Some(dt(2026, 7, 11, 6)),
                &["acme"],
                &["memorandum"],
                &["ningbo"],
                false,
                0.62,
            ),
            None,
        );

        let decision = match outcome {
            IncrementalAssignment::ReviewRequired(decision) => decision,
            other => panic!("expected review-required, got {other:?}"),
        };

        assert!(
            decision.confidence >= IncrementalClusteringConfig::default().review_threshold
                && decision.confidence < IncrementalClusteringConfig::default().auto_join_threshold
        );
        assert!(decision
            .reason_codes
            .iter()
            .any(|code| code == "review_required"));
        assert_eq!(clusterer.clusters().len(), 1);
        assert!(clusterer.clusters()[0].review_required);
        assert_eq!(clusterer.clusters()[0].mentions.len(), 2);
    }

    #[test]
    fn higher_scoring_ineligible_candidate_does_not_mask_review_required_match() {
        let config = IncrementalClusteringConfig::default();
        let ineligible_cluster_id = Uuid::from_u128(1);
        let eligible_cluster_id = Uuid::from_u128(2);
        let mut clusterer = IncrementalClusterer {
            config: config.clone(),
            clusters: vec![
                candidate_cluster(
                    ineligible_cluster_id,
                    None,
                    vec![cluster_mention(
                        ineligible_cluster_id,
                        None,
                        mention(
                            "ACME memorandum update",
                            Some(dt(2026, 7, 11, 5)),
                            &["acme"],
                            &["bulletin"],
                            &["ningbo"],
                            true,
                            0.94,
                        ),
                    )],
                    false,
                ),
                candidate_cluster(
                    eligible_cluster_id,
                    None,
                    vec![cluster_mention(
                        eligible_cluster_id,
                        None,
                        mention(
                            "ACME signs logistics memorandum",
                            Some(dt(2026, 7, 10, 8)),
                            &["acme", "logisticsco"],
                            &["sign", "memorandum"],
                            &["ningbo"],
                            true,
                            0.93,
                        ),
                    )],
                    false,
                ),
            ],
        };

        let outcome = clusterer.ingest_mention(
            mention(
                "ACME memorandum update",
                Some(dt(2026, 7, 11, 6)),
                &["acme"],
                &["memorandum"],
                &["ningbo"],
                false,
                0.62,
            ),
            None,
        );

        let decision = match outcome {
            IncrementalAssignment::ReviewRequired(decision) => decision,
            other => panic!("expected review-required for eligible cluster, got {other:?}"),
        };

        assert_eq!(decision.event_cluster_id, eligible_cluster_id);
        assert!(decision.confidence >= config.review_threshold);
        assert!(decision.confidence < config.auto_join_threshold);
        assert_eq!(
            clusterer
                .clusters()
                .iter()
                .find(|cluster| cluster.event_cluster_id == ineligible_cluster_id)
                .unwrap()
                .mentions
                .len(),
            1
        );
        assert!(
            clusterer
                .clusters()
                .iter()
                .find(|cluster| cluster.event_cluster_id == eligible_cluster_id)
                .unwrap()
                .review_required
        );
    }

    #[test]
    fn duplicate_group_members_do_not_count_as_independent_sources() {
        let mut clusterer = IncrementalClusterer::new(IncrementalClusteringConfig::default());
        let duplicate_group_id = Uuid::new_v4();

        clusterer.ingest_mention(
            mention(
                "ACME wins municipal contract",
                Some(dt(2026, 7, 10, 8)),
                &["acme"],
                &["win", "contract"],
                &["beijing"],
                true,
                0.95,
            ),
            Some(duplicate_group_id),
        );
        clusterer.ingest_mention(
            mention(
                "Municipal contract awarded to ACME",
                Some(dt(2026, 7, 10, 9)),
                &["acme"],
                &["award", "contract"],
                &["beijing"],
                false,
                0.77,
            ),
            Some(duplicate_group_id),
        );

        let cluster = &clusterer.clusters()[0];

        assert_eq!(cluster.mentions.len(), 2);
        assert_eq!(cluster.independent_sources(), 1);
        assert_eq!(cluster.source_entropy(), 0.0);
    }

    #[test]
    fn end_of_day_refinement_merges_fragmented_clusters_and_preserves_duplicate_groups() {
        let left_cluster_id = Uuid::new_v4();
        let right_cluster_id = Uuid::new_v4();
        let shared_duplicate_group_id = Uuid::new_v4();
        let clusters = vec![
            candidate_cluster(
                left_cluster_id,
                None,
                vec![cluster_mention(
                    left_cluster_id,
                    Some(shared_duplicate_group_id),
                    mention(
                        "ACME expands battery line",
                        Some(dt(2026, 7, 10, 8)),
                        &["acme"],
                        &["expand", "battery"],
                        &["suzhou"],
                        true,
                        0.91,
                    ),
                )],
                false,
            ),
            candidate_cluster(
                right_cluster_id,
                None,
                vec![cluster_mention(
                    right_cluster_id,
                    None,
                    mention(
                        "ACME battery expansion confirmed",
                        Some(dt(2026, 7, 10, 11)),
                        &["acme"],
                        &["expand", "battery"],
                        &["suzhou"],
                        true,
                        0.83,
                    ),
                )],
                true,
            ),
        ];

        let refined = EndOfDayRefiner::new(IncrementalClusteringConfig::default())
            .refine(&clusters, &LockedClusterRelations::default());

        assert_eq!(refined.len(), 1);
        let duplicate_group_ids = refined[0]
            .mentions
            .iter()
            .filter_map(|mention| mention.duplicate_group_id)
            .collect::<Vec<_>>();
        assert_eq!(duplicate_group_ids, vec![shared_duplicate_group_id]);
        assert_eq!(refined[0].mentions.len(), 2);
        assert_eq!(refined[0].independent_sources, 2);
    }

    #[test]
    fn end_of_day_refinement_splits_overbroad_clusters_and_respects_locked_split_relations() {
        let origin_cluster_id = Uuid::new_v4();
        let second_origin_cluster_id = Uuid::new_v4();
        let clusters = vec![
            candidate_cluster(
                origin_cluster_id,
                Some(4),
                vec![
                    cluster_mention(
                        origin_cluster_id,
                        None,
                        mention(
                            "ACME announces dividend",
                            Some(dt(2026, 7, 10, 8)),
                            &["acme"],
                            &["announce", "dividend"],
                            &["shanghai"],
                            true,
                            0.92,
                        ),
                    ),
                    cluster_mention(
                        origin_cluster_id,
                        None,
                        mention(
                            "ACME chief executive resigns",
                            Some(dt(2026, 7, 10, 9)),
                            &["acme"],
                            &["resign", "executive"],
                            &["shanghai"],
                            true,
                            0.88,
                        ),
                    ),
                ],
                false,
            ),
            candidate_cluster(
                second_origin_cluster_id,
                Some(2),
                vec![cluster_mention(
                    second_origin_cluster_id,
                    None,
                    mention(
                        "ACME dividend details updated",
                        Some(dt(2026, 7, 10, 10)),
                        &["acme"],
                        &["announce", "dividend"],
                        &["shanghai"],
                        false,
                        0.73,
                    ),
                )],
                false,
            ),
        ];

        let mut locked = LockedClusterRelations::default();
        locked
            .split_pairs
            .insert(ordered_pair(origin_cluster_id, second_origin_cluster_id));

        let refined =
            EndOfDayRefiner::new(IncrementalClusteringConfig::default()).refine(&clusters, &locked);

        assert_eq!(refined.len(), 3);
        assert!(refined.iter().all(|cluster| !cluster.mentions.is_empty()));
        assert!(refined.iter().any(|cluster| cluster.mentions.len() == 1
            && cluster.mentions[0].origin_cluster_id == second_origin_cluster_id));
    }

    #[test]
    fn end_of_day_refinement_respects_locked_merge_relations() {
        let left_cluster_id = Uuid::new_v4();
        let right_cluster_id = Uuid::new_v4();
        let clusters = vec![
            candidate_cluster(
                left_cluster_id,
                Some(5),
                vec![cluster_mention(
                    left_cluster_id,
                    None,
                    mention(
                        "ACME updates bonded debt terms",
                        Some(dt(2026, 7, 10, 8)),
                        &["acme"],
                        &["update", "debt"],
                        &["xiamen"],
                        false,
                        0.63,
                    ),
                )],
                false,
            ),
            candidate_cluster(
                right_cluster_id,
                Some(1),
                vec![cluster_mention(
                    right_cluster_id,
                    None,
                    mention(
                        "ACME debt revision filing",
                        Some(dt(2026, 7, 10, 12)),
                        &["acme"],
                        &["revise", "debt"],
                        &["xiamen"],
                        false,
                        0.58,
                    ),
                )],
                true,
            ),
        ];
        let mut locked = LockedClusterRelations::default();
        locked
            .merge_pairs
            .insert(ordered_pair(left_cluster_id, right_cluster_id));

        let refined =
            EndOfDayRefiner::new(IncrementalClusteringConfig::default()).refine(&clusters, &locked);

        assert_eq!(refined.len(), 1);
        assert_eq!(refined[0].mentions.len(), 2);
    }

    #[test]
    fn end_of_day_refinement_assigns_initial_version_to_new_clusters() {
        let cluster_id = Uuid::new_v4();
        let clusters = vec![candidate_cluster(
            cluster_id,
            None,
            vec![cluster_mention(
                cluster_id,
                None,
                mention(
                    "ACME opens a new production line",
                    Some(dt(2026, 7, 10, 8)),
                    &["acme"],
                    &["open", "production"],
                    &["suzhou"],
                    true,
                    0.91,
                ),
            )],
            false,
        )];

        let refined = EndOfDayRefiner::new(IncrementalClusteringConfig::default())
            .refine(&clusters, &LockedClusterRelations::default());

        assert_eq!(refined.len(), 1);
        assert_eq!(refined[0].event_cluster_id, cluster_id);
        assert_eq!(refined[0].cluster_version, 1);
        assert_eq!(refined[0].supersedes_version, None);
        assert_eq!(
            refined[0].input_cluster_versions,
            vec![cluster_version_ref(cluster_id, None)]
        );
    }

    #[test]
    fn end_of_day_refinement_increments_existing_cluster_versions_and_tracks_inputs() {
        let retained_cluster_id = Uuid::from_u128(10);
        let merged_cluster_id = Uuid::from_u128(20);
        let clusters = vec![
            candidate_cluster(
                retained_cluster_id,
                Some(7),
                vec![cluster_mention(
                    retained_cluster_id,
                    None,
                    mention(
                        "ACME expands battery output",
                        Some(dt(2026, 7, 10, 8)),
                        &["acme"],
                        &["expand", "battery"],
                        &["suzhou"],
                        true,
                        0.94,
                    ),
                )],
                false,
            ),
            candidate_cluster(
                merged_cluster_id,
                Some(2),
                vec![cluster_mention(
                    merged_cluster_id,
                    None,
                    mention(
                        "ACME battery expansion update",
                        Some(dt(2026, 7, 10, 9)),
                        &["acme"],
                        &["expand", "battery"],
                        &["suzhou"],
                        false,
                        0.75,
                    ),
                )],
                false,
            ),
        ];

        let refined = EndOfDayRefiner::new(IncrementalClusteringConfig::default())
            .refine(&clusters, &LockedClusterRelations::default());

        assert_eq!(refined.len(), 1);
        assert_eq!(refined[0].event_cluster_id, retained_cluster_id);
        assert_eq!(refined[0].cluster_version, 8);
        assert_eq!(refined[0].supersedes_version, Some(7));
        assert_eq!(
            refined[0].input_cluster_versions,
            vec![
                cluster_version_ref(retained_cluster_id, Some(7)),
                cluster_version_ref(merged_cluster_id, Some(2)),
            ]
        );
    }

    fn candidate_cluster(
        event_cluster_id: Uuid,
        cluster_version: Option<i32>,
        mentions: Vec<ClusterMention>,
        review_required: bool,
    ) -> CandidateCluster {
        CandidateCluster {
            event_cluster_id,
            cluster_version,
            input_cluster_versions: vec![cluster_version_ref(event_cluster_id, cluster_version)],
            mentions,
            review_required,
        }
    }

    fn cluster_version_ref(
        event_cluster_id: Uuid,
        cluster_version: Option<i32>,
    ) -> ClusterVersionRef {
        ClusterVersionRef {
            event_cluster_id,
            cluster_version,
        }
    }

    fn cluster_mention(
        origin_cluster_id: Uuid,
        duplicate_group_id: Option<Uuid>,
        mention: EventMention,
    ) -> ClusterMention {
        ClusterMention {
            origin_cluster_id,
            duplicate_group_id,
            mention,
        }
    }

    fn mention(
        label: &str,
        event_time: Option<chrono::DateTime<Utc>>,
        entity_ids: &[&str],
        action_tokens: &[&str],
        location_tokens: &[&str],
        adds_new_fact: bool,
        source_independence: f64,
    ) -> EventMention {
        let semantic_vector = semantic_vector(label);
        EventMention {
            mention_id: Uuid::new_v4(),
            evidence_id: Uuid::new_v4(),
            event_time,
            entity_ids: entity_ids
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
            action_tokens: action_tokens
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
            location_tokens: location_tokens
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
            semantic_vector,
            adds_new_fact,
            source_independence,
        }
    }

    fn semantic_vector(label: &str) -> Vec<f32> {
        let lowered = label.to_lowercase();
        let contract = if lowered.contains("contract") {
            1.0
        } else {
            0.0
        };
        let award = if lowered.contains("award") || lowered.contains("wins") {
            1.0
        } else {
            0.0
        };
        let expansion = if lowered.contains("expand") || lowered.contains("expansion") {
            1.0
        } else {
            0.0
        };
        let dividend = if lowered.contains("dividend") {
            1.0
        } else {
            0.0
        };
        let executive = if lowered.contains("executive") || lowered.contains("resigns") {
            1.0
        } else {
            0.0
        };
        let debt = if lowered.contains("debt") || lowered.contains("bonded") {
            1.0
        } else {
            0.0
        };
        let memorandum = if lowered.contains("memorandum") {
            1.0
        } else {
            0.2
        };
        let time_bias = 1.0 - ((label.len() % 5) as f32 * 0.05);

        vec![
            contract, award, expansion, dividend, executive, debt, memorandum, time_bias,
        ]
    }

    fn ordered_pair(left: Uuid, right: Uuid) -> (Uuid, Uuid) {
        if left <= right {
            (left, right)
        } else {
            (right, left)
        }
    }

    fn dt(year: i32, month: u32, day: u32, hour: u32) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, 0, 0).unwrap()
    }

    #[test]
    fn semantic_similarity_rejects_mismatched_vector_dimensions() {
        assert_eq!(semantic_similarity(&[1.0, 0.0, 0.5], &[1.0, 0.0]), 0.0);
    }

    #[test]
    fn semantic_vector_fixture_keeps_close_texts_close() {
        let left = semantic_vector("ACME contract award");
        let right = semantic_vector("ACME wins contract award");
        let distance = left
            .iter()
            .zip(right.iter())
            .map(|(left, right)| (left - right).abs())
            .sum::<f32>();

        assert!(distance < 0.15);
    }

    #[test]
    fn semantic_vector_fixture_separates_distinct_actions() {
        let left = semantic_vector("ACME announces dividend");
        let right = semantic_vector("ACME chief executive resigns");

        let distance = left
            .iter()
            .zip(right.iter())
            .map(|(left, right)| (left - right).abs())
            .sum::<f32>();

        assert!(distance > 1.5);
    }

    #[test]
    fn time_fixture_creates_review_window_gap() {
        let delta = dt(2026, 7, 11, 6) - dt(2026, 7, 10, 8);
        assert_eq!(delta, Duration::hours(22));
    }
}
