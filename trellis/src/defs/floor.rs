//! Definition floors (ADR-0024): the oldest Trellis release that can run a
//! definition.
//!
//! Every stored definition records its floor in
//! `transform_definitions.floor_version`. It is the highest release any
//! [`Feature`] the definition uses arrived in: the grammar constructs of its
//! parsed tree, its key space and options, whether a table it reads is
//! partitioned, the hidden-table layout and per-definition state this
//! release writes for it, and every argument value, type rule and key type
//! the validator admitted it by over its columns' types. [`Floors::of`] computes
//! it, and [`stamp`] writes it wherever Trellis writes the definition
//! (define, `ALTER TRANSFORM`), recreates its hidden tables (a resume's
//! rebuild), or sees a column it reads change type while the validator
//! still admits it. A pause or a read never changes it, and it can go down:
//! an `ALTER` that removes the only newer feature lowers it, and so can a
//! retype that changes which type rule admits an expression.
//!
//! Loading a stored definition only parses it, so the tree alone can't tell
//! an older release that a value or an operand type it would refuse is in
//! use. The validation can: each value ([`super::registry::ArgValueSpec`])
//! and each case of a type rule ([`super::registry::TypeRule`]) declares
//! its release, a rule returns its case as a required part of its result
//! ([`super::registry::Admitted`]), and reading the result [`record`]s it
//! while [`validated_features`] runs the validation. A key type is the same:
//! each `GROUP BY` key type is a [`super::registry::GroupByKeyRule`] case of a
//! type rule, and each `JOIN ... ON` key type an entry of the join-key
//! allowlist ([`super::join_key_types::JoinKey`]), recorded where the
//! validator admits the key. The keys of the tables it reads are admitted
//! away from the validator, over the live catalog: a primary key's type by
//! the same allowlist entry ([`super::ddl::admitted_primary_key`]) and a
//! partition key's by the partition-key type list
//! ([`super::partition_tree::PartitionTree::key_types`]), both read by
//! [`live_key_features`], which async code can't record into the collector.
//!
//! The registry is exhaustive by construction. [`features_used`] matches
//! every node kind of the tree, and [`Feature::since`] matches every
//! feature, so a new node kind, key space, option or layout can't compile
//! without declaring the release it arrived in. A function, typed literal,
//! `CAST` target or argument value declares it in its own registry entry
//! ([`super::registry::FunctionSpec::since`],
//! [`super::typed_literal::TypedLiteralSpec::since`],
//! [`super::registry::CastTargetSpec::since`],
//! [`super::registry::ArgValueSpec::since`]), and a type rule's case in
//! [`super::registry::TypeRule::since`] (a join-key type in
//! [`super::join_key_types::JoinKey::since`]). Everything that exists when floors
//! were introduced has floor [`BASELINE`].
//!
//! A definition above the running release, or written under a grammar newer
//! than this binary reads, is one this binary can't run ([`Unrunnable`],
//! ADR-0024 rule 3). [`Floors::unrunnable`] decides it from the stored
//! columns alone, before anything parses the text: every path that loads a
//! stored definition skips such a one ([`super::grammar::load_stored`]),
//! attaching pauses it with [`Unrunnable::reason`], and `RESUME`, `ALTER`
//! and `DROP` refuse it.

use std::fmt;
use std::sync::Arc;

use super::ast::{
    Expr, FieldDef, JoinKind, JoinOn, JoinSpec, KeySpace, Operator, Predicate, TransformDef,
    ValueType,
};

/// A Trellis release version, `major.minor.patch`, ordered as semver orders
/// releases. A floor is always a release, never a pre-release.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Release {
    major: u64,
    minor: u64,
    patch: u64,
}

impl Release {
    /// The release `major.minor.patch`.
    pub const fn new(major: u64, minor: u64, patch: u64) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    /// Reads a release spelled exactly `major.minor.patch` (each part a
    /// decimal number without a leading zero), as `floor_version` stores it.
    /// `None` for anything else, a pre-release included.
    pub fn parse(text: &str) -> Option<Self> {
        let mut parts = text.split('.');
        let mut part = || {
            let part = parts.next()?;
            let well_formed = !part.is_empty()
                && part.bytes().all(|b| b.is_ascii_digit())
                && (part == "0" || !part.starts_with('0'));
            if well_formed { part.parse().ok() } else { None }
        };
        let release = Self::new(part()?, part()?, part()?);
        parts.next().is_none().then_some(release)
    }

    /// The release a binary's version belongs to: its `major.minor.patch`,
    /// without a pre-release tag or build metadata (`0.3.0-rc.1` belongs to
    /// `0.3.0`). `None` when the version doesn't start with a release.
    pub(crate) fn of_binary(version: &str) -> Option<Self> {
        let end = version.find(['-', '+']).unwrap_or(version.len());
        Self::parse(&version[..end])
    }
}

impl fmt::Display for Release {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// The floor of every feature that exists when floors are introduced, and
/// of every definition the migration that adds them backfills: 0.2.0, the
/// first release that can be rolled back to (ADR-0024 rule 8). No earlier
/// binary carries this code, so the running release is never below it.
pub const BASELINE: Release = Release::new(0, 2, 0);

/// Which key space a definition is (see [`KeySpace`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeySpaceKind {
    OneToOne,
    Aggregate,
    Join,
    JoinOneToOne,
}

/// Which predicate a definition has (see [`Predicate`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredicateKind {
    /// None, or `WHERE TRUE`.
    True,
}

/// A kind of expression node whose since has no further detail: the nodes
/// of [`Expr`] that aren't an operator, a function call, a typed literal or
/// a cast, which carry their own [`Feature`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExprNode {
    Column,
    NumberLiteral,
    StringLiteral,
    BooleanLiteral,
    Null,
    SourceColumn,
    Negate,
    Not,
    IsNull,
    Case,
}

/// The hidden tables and per-definition state Trellis keeps for a
/// definition, in the layout this release writes. A release that changes a
/// layout adds a variant for the new one, with its own since, and
/// [`Layout::of`] returns it from then on: define and a resume's rebuild
/// write the new layout, so they stamp its floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layout {
    /// The catalog rows every definition has: its `transform_definitions`
    /// row, its field and column-status rows, its recorded column and key
    /// types.
    DefinitionState,
    /// A 1-1 target's ledger.
    OneToOneLedger,
    /// An aggregate's group-basis partials table (`agg_<id>`).
    GroupBasis,
    /// An aggregate `JOIN`'s hidden tables (`join_<id>_*`).
    Join,
    /// A 1-1 `JOIN`'s hidden tables (`join1_<id>_*`) and pending fan-outs.
    JoinOneToOne,
}

impl Layout {
    /// The layouts this release writes for a definition of `key_space`.
    pub(crate) fn of(key_space: &KeySpace) -> [Layout; 2] {
        let hidden = match key_space {
            KeySpace::OneToOne => Layout::OneToOneLedger,
            KeySpace::Aggregate { .. } => Layout::GroupBasis,
            KeySpace::Join(_) => Layout::Join,
            KeySpace::JoinOneToOne(_) => Layout::JoinOneToOne,
        };
        [Layout::DefinitionState, hidden]
    }
}

/// One thing a definition can use that a release may lack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Feature {
    /// Its key space.
    KeySpace(KeySpaceKind),
    /// A `JOIN`'s kind.
    JoinKind(JoinKind),
    /// A table (the target, the `FROM` table or the joined table) named
    /// with its schema.
    QualifiedTable,
    /// Its predicate.
    Predicate(PredicateKind),
    /// `WITHOUT SOURCE INDEX`.
    WithoutSourceIndex,
    /// `ADOPT EXISTING TABLE`: define builds into an existing empty target.
    AdoptTarget,
    /// A `JOIN`'s table given an alias (`FROM <table> [AS] <alias>`).
    TableAlias,
    /// A table it reads is partitioned (ADR-0017).
    PartitionedSource,
    /// An expression node of this kind.
    Expr(ExprNode),
    /// An operator.
    Operator(Operator),
    /// A function, aggregate or special form (`COALESCE`, `NULLIF`,
    /// `EXTRACT`) by its canonical name.
    Function(&'static str),
    /// A typed literal of this type.
    TypedLiteral(ValueType),
    /// A `CAST` to this type.
    CastTarget(ValueType),
    /// A hidden-table layout or per-definition state this release writes.
    Layout(Layout),
    /// A case of a type rule the validator admitted the definition's operand
    /// types by ([`super::registry::TypeRule`]).
    TypeRule(super::registry::TypeRule),
    /// An argument value the validator admitted: an `EXTRACT` field, a
    /// `DATE_TRUNC` unit or an `AT TIME ZONE` zone
    /// ([`super::registry::ArgValue`]).
    Value(super::registry::ArgValue),
    /// A type admitted as a `JOIN ... ON` key, or as the primary key of a
    /// table the definition reads ([`super::join_key_types::JoinKey`]).
    JoinKey(super::join_key_types::JoinKey),
    /// A type admitted as the partition key of a partitioned table the
    /// definition reads, by its name in the supported list
    /// ([`super::partition_tree::PartitionTree::key_types`]).
    PartitionKeyType(&'static str),
}

/// The special forms the parser builds as a [`Expr::FunctionCall`] without
/// a [`super::registry::FunctionSpec`], and the release each arrived in.
const SPECIAL_FORMS: &[(&str, Release)] = &[
    ("COALESCE", BASELINE),
    ("NULLIF", BASELINE),
    ("EXTRACT", BASELINE),
];

impl Feature {
    /// The release this feature arrived in. `None` only for a name or type
    /// no registry entry declares, which a validated definition never uses;
    /// [`Floors::of`] counts it at the running release.
    pub fn since(&self) -> Option<Release> {
        match *self {
            Feature::KeySpace(kind) => Some(match kind {
                KeySpaceKind::OneToOne
                | KeySpaceKind::Aggregate
                | KeySpaceKind::Join
                | KeySpaceKind::JoinOneToOne => BASELINE,
            }),
            Feature::JoinKind(kind) => Some(match kind {
                JoinKind::Inner | JoinKind::Left => BASELINE,
            }),
            Feature::QualifiedTable | Feature::WithoutSourceIndex | Feature::PartitionedSource => {
                Some(BASELINE)
            }
            // Arrives in 0.2.0, the release floors arrive in.
            Feature::AdoptTarget => Some(BASELINE),
            // Arrives in 0.2.0, the release floors arrive in.
            Feature::TableAlias => Some(BASELINE),
            Feature::Predicate(kind) => Some(match kind {
                PredicateKind::True => BASELINE,
            }),
            Feature::Expr(node) => Some(match node {
                ExprNode::Column
                | ExprNode::NumberLiteral
                | ExprNode::StringLiteral
                | ExprNode::BooleanLiteral
                | ExprNode::Null
                | ExprNode::SourceColumn
                | ExprNode::Negate
                | ExprNode::Not
                | ExprNode::IsNull
                | ExprNode::Case => BASELINE,
            }),
            Feature::Operator(operator) => Some(match operator {
                Operator::Add
                | Operator::Subtract
                | Operator::Multiply
                | Operator::Divide
                | Operator::Modulo
                | Operator::GreaterThan
                | Operator::GreaterThanOrEqual
                | Operator::LessThan
                | Operator::LessThanOrEqual
                | Operator::Equal
                | Operator::NotEqual
                | Operator::IsDistinctFrom
                | Operator::IsNotDistinctFrom
                | Operator::And
                | Operator::Or
                | Operator::AtTimeZone => BASELINE,
            }),
            Feature::Function(name) => function_since(name),
            Feature::TypedLiteral(value_type) => super::typed_literal::TYPED_LITERALS
                .iter()
                .find(|spec| spec.value_type == value_type)
                .map(|spec| spec.since),
            Feature::CastTarget(value_type) => super::registry::CAST_TARGETS
                .iter()
                .chain(super::registry::NULL_CAST_TARGETS)
                .filter(|spec| spec.value_type == value_type)
                .map(|spec| spec.since)
                .max(),
            Feature::Layout(layout) => Some(match layout {
                Layout::DefinitionState
                | Layout::OneToOneLedger
                | Layout::GroupBasis
                | Layout::Join
                | Layout::JoinOneToOne => BASELINE,
            }),
            Feature::TypeRule(rule) => Some(rule.since()),
            Feature::Value(value) => value.since(),
            Feature::JoinKey(key) => key.since(),
            Feature::PartitionKeyType(name) => super::partition_tree::key_type_since(name),
        }
    }

    /// The name a test's override (`Config::with_feature_since`) addresses this
    /// feature by: its `Debug` form, `Expr(Case)` or `Function("ABS")`.
    fn key(&self) -> String {
        format!("{self:?}")
    }
}

/// The release function `name` arrived in: the latest of its overloads',
/// since a call's tree doesn't record which overload it resolved to.
fn function_since(name: &str) -> Option<Release> {
    latest_overload(
        name,
        super::registry::FUNCTIONS
            .iter()
            .chain(super::registry::AGGREGATE_FUNCTION_SPECS),
    )
    .into_iter()
    .chain(
        SPECIAL_FORMS
            .iter()
            .filter(|(form, _)| *form == name)
            .map(|&(_, since)| since),
    )
    .max()
}

/// The latest since of every entry of `specs` named `name`.
fn latest_overload<'a>(
    name: &str,
    specs: impl Iterator<Item = &'a super::registry::FunctionSpec>,
) -> Option<Release> {
    specs
        .filter(|spec| spec.name == name)
        .map(|spec| spec.since)
        .max()
}

/// The registry's canonical `&'static` spelling of function `name`, so a
/// [`Feature::Function`] is `Copy`. `None` for a name no entry declares.
fn function_name(name: &str) -> Option<&'static str> {
    super::registry::FUNCTIONS
        .iter()
        .chain(super::registry::AGGREGATE_FUNCTION_SPECS)
        .find(|spec| spec.name == name)
        .map(|spec| spec.name)
        .or_else(|| {
            SPECIAL_FORMS
                .iter()
                .find(|(form, _)| *form == name)
                .map(|&(form, _)| form)
        })
}

/// Everything `def` uses, each once, in the order first met: its key space
/// and options, every node of every field's expression, and the layouts
/// this release writes for it. `partitioned` is whether a table it reads is
/// partitioned, which only the database knows. A function name no registry
/// entry declares is left out and reported in the second value.
pub(crate) fn features_used(def: &TransformDef, partitioned: bool) -> (Vec<Feature>, bool) {
    // Destructured whole, as is every part of the tree below, so a field
    // added to the definition doesn't compile until it is counted here.
    let TransformDef {
        target: _,
        explicit_target_schema,
        source: _,
        explicit_source_schema,
        key_space,
        fields,
        predicate,
        without_source_index,
        adopt_target,
    } = def;
    let mut used = Used::default();
    let (kind, join) = match key_space {
        KeySpace::OneToOne => (KeySpaceKind::OneToOne, None),
        KeySpace::Aggregate { group_by: _ } => (KeySpaceKind::Aggregate, None),
        KeySpace::Join(spec) => (KeySpaceKind::Join, Some(spec)),
        KeySpace::JoinOneToOne(spec) => (KeySpaceKind::JoinOneToOne, Some(spec)),
    };
    used.add(Feature::KeySpace(kind));
    if let Some(JoinSpec {
        kind,
        source_alias,
        right: _,
        explicit_right_schema,
        right_alias,
        on,
        group_by: _,
    }) = join
    {
        used.add(Feature::JoinKind(*kind));
        if explicit_right_schema.is_some() {
            used.add(Feature::QualifiedTable);
        }
        if source_alias.is_some() || right_alias.is_some() {
            used.add(Feature::TableAlias);
        }
        // An `ON` term is two columns, which every `JOIN` has.
        for JoinOn { lhs: _, rhs: _ } in on {}
    }
    if explicit_target_schema.is_some() || explicit_source_schema.is_some() {
        used.add(Feature::QualifiedTable);
    }
    used.add(Feature::Predicate(match predicate {
        Predicate::True => PredicateKind::True,
    }));
    if *without_source_index {
        used.add(Feature::WithoutSourceIndex);
    }
    if *adopt_target {
        used.add(Feature::AdoptTarget);
    }
    if partitioned {
        used.add(Feature::PartitionedSource);
    }
    for FieldDef { name: _, expr } in fields {
        used.expr(expr);
    }
    for layout in Layout::of(key_space) {
        used.add(Feature::Layout(layout));
    }
    (used.features, used.unknown)
}

#[derive(Default)]
struct Used {
    features: Vec<Feature>,
    unknown: bool,
}

impl Used {
    fn add(&mut self, feature: Feature) {
        if !self.features.contains(&feature) {
            self.features.push(feature);
        }
    }

    fn expr(&mut self, expr: &Expr) {
        match expr {
            Expr::Column(_) => self.add(Feature::Expr(ExprNode::Column)),
            Expr::NumberLiteral(_) => self.add(Feature::Expr(ExprNode::NumberLiteral)),
            Expr::StringLiteral(_) => self.add(Feature::Expr(ExprNode::StringLiteral)),
            Expr::TypedLiteral {
                value_type,
                text: _,
            } => self.add(Feature::TypedLiteral(*value_type)),
            Expr::BooleanLiteral(_) => self.add(Feature::Expr(ExprNode::BooleanLiteral)),
            Expr::Null => self.add(Feature::Expr(ExprNode::Null)),
            Expr::SourceColumn {
                schema: _,
                table: _,
                column: _,
            } => self.add(Feature::Expr(ExprNode::SourceColumn)),
            Expr::BinaryOp { op, lhs, rhs } => {
                self.add(Feature::Operator(*op));
                self.expr(lhs);
                self.expr(rhs);
            }
            Expr::FunctionCall { name, args } => {
                match function_name(name) {
                    Some(name) => self.add(Feature::Function(name)),
                    None => self.unknown = true,
                }
                for arg in args {
                    self.expr(arg);
                }
            }
            Expr::Negate(operand) => {
                self.add(Feature::Expr(ExprNode::Negate));
                self.expr(operand);
            }
            Expr::Cast { expr, target } => {
                self.add(Feature::CastTarget(*target));
                self.expr(expr);
            }
            Expr::Not(operand) => {
                self.add(Feature::Expr(ExprNode::Not));
                self.expr(operand);
            }
            Expr::IsNull { expr, negated: _ } => {
                self.add(Feature::Expr(ExprNode::IsNull));
                self.expr(expr);
            }
            Expr::Case {
                branches,
                otherwise,
            } => {
                self.add(Feature::Expr(ExprNode::Case));
                for (condition, result) in branches {
                    self.expr(condition);
                    self.expr(result);
                }
                if let Some(otherwise) = otherwise {
                    self.expr(otherwise);
                }
            }
        }
    }
}

thread_local! {
    /// The features the validation running on this thread has used so far,
    /// while [`collect`] runs it; `None` outside one.
    static VALIDATED: std::cell::RefCell<Option<Vec<Feature>>> =
        const { std::cell::RefCell::new(None) };
}

/// Records that the validation running on this thread used `feature`: a
/// type rule's case or an argument value. A no-op outside [`collect`], so a
/// rule called for any other reason (a generator, a renderer) records
/// nothing.
pub(crate) fn record(feature: Feature) {
    VALIDATED.with(|used| {
        if let Some(used) = used.borrow_mut().as_mut()
            && !used.contains(&feature)
        {
            used.push(feature);
        }
    });
}

/// Runs `validate` (synchronous, so it stays on this thread) and returns its
/// result with every feature it [`record`]ed, each once. Validation is a
/// pure function of the tree and the column types, so the same definition
/// over the same types always records the same features.
pub(crate) fn collect<T>(validate: impl FnOnce() -> T) -> (T, Vec<Feature>) {
    /// Puts back the collection this one interrupted, also on unwind.
    struct Restore(Option<Option<Vec<Feature>>>);
    impl Drop for Restore {
        fn drop(&mut self) {
            if let Some(outer) = self.0.take() {
                VALIDATED.with(|used| *used.borrow_mut() = outer);
            }
        }
    }
    let outer = VALIDATED.with(|cell| cell.replace(Some(Vec::new())));
    let mut restore = Restore(Some(outer));
    let result = validate();
    let used = VALIDATED.with(|cell| cell.take()).unwrap_or_default();
    // A nested validation's features are the outer one's too.
    let outer = restore.0.take().expect("restored once").map(|mut outer| {
        for feature in &used {
            if !outer.contains(feature) {
                outer.push(*feature);
            }
        }
        outer
    });
    VALIDATED.with(|cell| cell.replace(outer));
    (result, used)
}

/// The value and type rules define's validation admits `def` by, over the
/// live columns of the table it reads (`from`) and, for a `JOIN`, of the
/// joined table: the validation define and a resume run on the same types
/// ([`super::validate::validate`] or [`super::validate::validate_join`],
/// and the aggregate family check), collected with [`collect`]. The
/// validator's refusal when it no longer admits them.
pub(crate) fn validated_features(
    def: &TransformDef,
    from: &std::collections::HashMap<String, super::validate::JoinColumnInfo>,
    joined: Option<&std::collections::HashMap<String, super::validate::JoinColumnInfo>>,
) -> Result<Vec<Feature>, super::validate::ValidationError> {
    let (result, used) = collect(|| match joined {
        Some(joined) => super::validate::validate_join(def, from, joined),
        None => {
            let value_types = from
                .iter()
                .map(|(name, info)| (name.clone(), info.value_type))
                .collect();
            super::validate::validate(def, &value_types)?;
            super::validate::validate_aggregate_admitted(def, from)
        }
    });
    result.map(|()| used)
}

/// What a floor is computed against: the running release and, in tests, the
/// since a feature is given in place of its registry entry's. Built from a
/// [`crate::config::Config`] and carried by each [`crate::pool::Pool`].
#[derive(Debug, Clone)]
pub struct Floors {
    /// The release this binary belongs to, never below [`BASELINE`]; `None`
    /// when its version doesn't start with a release.
    running: Option<Release>,
    /// Features given another since, by [`Feature::key`].
    overrides: Arc<[(String, Release)]>,
}

impl Floors {
    /// The floors of a binary running as `binary_version`.
    ///
    /// A binary is never below [`BASELINE`], the first release carrying
    /// floors: a development build whose manifest still carries the previous
    /// release's version belongs to the next one.
    pub(crate) fn new(binary_version: &str, overrides: Arc<[(String, Release)]>) -> Self {
        Self {
            running: Release::of_binary(binary_version).map(|release| release.max(BASELINE)),
            overrides,
        }
    }

    /// The floors of this build, with no overrides: a test's, which
    /// injects no version.
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) fn of_this_binary() -> Self {
        Self::new(crate::handover::BINARY_VERSION, Arc::from([]))
    }

    /// The running release when a definition whose stored floor is `stored`
    /// is above it, so this binary can't run the definition and must not
    /// drop it (ADR-0024 rules 3 and 5); `None` when the binary meets the
    /// floor. A floor that doesn't read as a release is above it: an unknown
    /// stored value never falls through to a default (rule 6). A binary
    /// whose version isn't a release meets every floor.
    pub(crate) fn above_running(&self, stored: &str) -> Option<Release> {
        let running = self.running?;
        match Release::parse(stored) {
            Some(floor) if floor <= running => None,
            _ => Some(running),
        }
    }

    /// Why this binary can't run a definition whose stored floor is `floor`
    /// and whose text was written under grammar `grammar_version`, or `None`
    /// when it can (ADR-0024 rule 3). Decided from the stored columns alone,
    /// so it is asked before the text is parsed.
    pub(crate) fn unrunnable(&self, floor: &str, grammar_version: i32) -> Option<Unrunnable> {
        self.unrunnable_in(
            floor,
            grammar_version,
            super::grammar::CURRENT_GRAMMAR_VERSION,
        )
    }

    /// [`Floors::unrunnable`] for a binary whose newest grammar is `current`.
    /// The floor is asked first: a definition above it names the release to
    /// roll forward to.
    pub(crate) fn unrunnable_in(
        &self,
        floor: &str,
        grammar_version: i32,
        current: i32,
    ) -> Option<Unrunnable> {
        if let Some(running) = self.above_running(floor) {
            return Some(Unrunnable::AboveFloor {
                floor: floor.to_string(),
                running,
            });
        }
        (grammar_version > current).then_some(Unrunnable::NewerGrammar {
            stored: grammar_version,
            current,
        })
    }

    /// The release `feature` arrived in, a test's override first.
    fn since(&self, feature: &Feature) -> Option<Release> {
        let key = feature.key();
        self.overrides
            .iter()
            .find(|(name, _)| *name == key)
            .map(|&(_, since)| since)
            .or_else(|| feature.since())
    }

    /// The floor of `def`, `partitioned` when a table it reads is: the
    /// latest since of every feature it uses, at most the running release.
    /// `validated` is what the validator admitted it by over its columns'
    /// types ([`validated_features`]), `None` when the validator refuses it,
    /// which counts at the running release.
    ///
    /// A binary only writes what it carries, so a floor above the running
    /// release is a registry entry declared for a later release than the
    /// binary's; capping it keeps the binary from writing a definition it
    /// would itself refuse to run. A feature no entry declares counts at the
    /// running release, never lower.
    pub(crate) fn of(
        &self,
        def: &TransformDef,
        partitioned: bool,
        validated: Option<&[Feature]>,
    ) -> Release {
        let (features, unknown) = features_used(def, partitioned);
        let ceiling = self
            .running
            .unwrap_or(Release::new(u64::MAX, u64::MAX, u64::MAX));
        let unknown = (unknown || validated.is_none()).then_some(ceiling);
        features
            .iter()
            .chain(validated.unwrap_or_default())
            .map(|feature| self.since(feature).unwrap_or(ceiling))
            .chain(unknown)
            .max()
            .unwrap_or(BASELINE)
            .min(ceiling)
    }
}

/// Why this binary can't run a stored definition (ADR-0024 rule 3), from
/// [`Floors::unrunnable`]. Such a definition is paused and never parsed: its
/// text may use what this release lacks, or mean something else under it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Unrunnable {
    /// Its floor is above the release this binary runs as.
    AboveFloor { floor: String, running: Release },
    /// Its text was written under a grammar newer than this binary reads
    /// (ADR-0018).
    NewerGrammar { stored: i32, current: i32 },
    /// A per-definition enum-like value stored for it is one this release
    /// doesn't know, so a newer release wrote it (ADR-0024 rule 6):
    /// `value` names the column and the value, as [`unknown_value_sql`]
    /// describes it.
    UnknownValue { value: String },
}

/// How every [`Unrunnable::reason`] starts, so a later attach finds the one
/// an earlier attach recorded and replaces it rather than adding another.
const UNRUNNABLE_PREFIX: &str = "this release can't run transform ";

/// What joins a pause's reason to the reason the definition was already
/// paused for, as the other pauses that keep an earlier reason write it
/// (`lifecycle::pause_for_relabel`).
const ALREADY_PAUSED: &str = " It was already paused: ";

impl Unrunnable {
    /// The `capture_failure` of definition `target` paused because this
    /// binary can't run it: what it needs, what this binary is, and the
    /// remedy. `RESUME` and `ALTER` refuse with it too.
    pub(crate) fn reason(&self, target: &str) -> String {
        match self {
            Unrunnable::AboveFloor { floor, running } => format!(
                "{UNRUNNABLE_PREFIX}{target}: its floor, the oldest Trellis release that can \
                 run it, is {floor}, and this binary runs release {running}; roll forward to \
                 {floor} or later, then RESUME it or DROP it there"
            ),
            Unrunnable::NewerGrammar { stored, current } => format!(
                "{UNRUNNABLE_PREFIX}{target}: it was written under grammar version {stored}, \
                 and this binary reads grammar versions up to {current}; roll forward to a \
                 release that reads it, then RESUME it or DROP it there"
            ),
            Unrunnable::UnknownValue { value } => format!(
                "{UNRUNNABLE_PREFIX}{target}: its {value} was written by a newer Trellis \
                 release, and this release doesn't know it; roll forward to the release that \
                 wrote it, then RESUME it or DROP it there"
            ),
        }
    }
}

/// SQL over a `transform_definitions` row aliased `d`: the first
/// per-definition enum-like value stored for it that this release doesn't
/// know, as `<table>.<column> '<value>'`, or `NULL` when it knows them all
/// (ADR-0024 rule 6). A newer release wrote such a value, so the definition
/// is paused ([`Unrunnable::UnknownValue`]) rather than read with a default.
/// Asked with the floor and grammar checks: at attach, at a takeover, before
/// each state-changing call, and by `RESUME` and `ALTER`, which refuse while
/// one is stored.
///
/// Writing a new value of any of these raises the definition floor of every
/// definition it is written for:
///
/// - `transform_definitions.status` ([`super::model::StoredStatus`]);
/// - `transform_definitions.build` (`staging::build::BUILD_REDERIVE`);
/// - `backfill_chunks.kind` (`super::chunk_queue::KIND_*`);
/// - the tag of a `backfill_chunks.lo` cursor
///   (`staging::build::unknown_cursor_tag_sql`): an untagged cursor is the
///   format every release reads;
/// - `partition_correction_progress.phase`
///   (`staging::partition_correction::PHASES`);
/// - `capture_failures.kind` ([`crate::CaptureFailureKind`]).
pub(crate) fn unknown_value_sql(d: &str) -> String {
    fn words<'a>(words: impl IntoIterator<Item = &'a str>) -> String {
        let words: Vec<&str> = words.into_iter().collect();
        format!("'{{{}}}'::text[]", words.join(","))
    }
    let status = words(super::model::StoredStatus::ALL.iter().map(|s| s.as_str()));
    let build = words([crate::staging::build::BUILD_REDERIVE]);
    let kinds = words(super::chunk_queue::KINDS);
    let phases = words(crate::staging::partition_correction::PHASES);
    let capture = words(crate::CaptureFailureKind::STORED.iter().map(|k| k.as_str()));
    let cursor = crate::staging::build::unknown_cursor_tag_sql(d, "bc");
    format!(
        "coalesce( \
             case when {d}.status <> all({status}) \
                  then 'transform_definitions.status ' || quote_literal({d}.status) end, \
             case when {d}.build <> all({build}) \
                  then 'transform_definitions.build ' || quote_literal({d}.build) end, \
             (select 'backfill_chunks.kind ' || quote_literal(bc.kind) from backfill_chunks bc \
              where bc.definition_id = {d}.id and bc.kind <> all({kinds}) \
              order by bc.id limit 1), \
             (select 'backfill_chunks.lo cursor tag ' || quote_literal({cursor}) \
              from backfill_chunks bc \
              where bc.definition_id = {d}.id and {cursor} is not null \
              order by bc.id limit 1), \
             (select 'partition_correction_progress.phase ' || quote_literal(p.phase) \
              from partition_correction_progress p \
              where p.definition_id = {d}.id and p.phase <> all({phases}) \
              order by p.correction_id limit 1), \
             (select 'capture_failures.kind ' || quote_literal(cf.kind) from capture_failures cf \
              where cf.transform_id = {d}.id and cf.kind <> all({capture})))"
    )
}

/// The `capture_failure` error a definition paused for `reason` (an
/// [`Unrunnable::reason`]) gets when its record holds `existing`, or `None`
/// when the record already says it. A reason an earlier attach recorded
/// (a different running release, say) is replaced, and every other reason is
/// kept after it (ADR-0024 rule 3: pausing an already-paused definition keeps
/// its reasons).
pub(crate) fn unrunnable_error(existing: Option<&str>, reason: &str) -> Option<String> {
    let Some(existing) = existing else {
        return Some(reason.to_string());
    };
    let earlier = if existing.starts_with(UNRUNNABLE_PREFIX) {
        existing
            .split_once(ALREADY_PAUSED)
            .map_or("", |(_, earlier)| earlier)
    } else {
        existing
    };
    let error = if earlier.is_empty() {
        reason.to_string()
    } else {
        format!("{reason}{ALREADY_PAUSED}{earlier}")
    };
    (error != existing).then_some(error)
}

/// The floor of `def`, which reads `source` and, for a `JOIN`, `joined`,
/// over those tables' columns as they are now: what define writes, and what
/// [`stamp`] stores.
pub(crate) async fn of_live(
    client: &impl tokio_postgres::GenericClient,
    floors: &Floors,
    def: &TransformDef,
    source: &str,
    joined: Option<&str>,
) -> Result<Release, super::catalog::CatalogError> {
    let tables: Vec<&str> = std::iter::once(source).chain(joined).collect();
    Ok(
        match on_live_retype(client, floors, def, source, joined).await? {
            OnRetype::Restamp(floor) => floor,
            // Validated just before in the same transaction, so not expected:
            // counted at the running release, never lower.
            OnRetype::Pause(_) => floors.of(def, any_partitioned(client, &tables).await?, None),
        },
    )
}

/// Whether any of `tables` (qualified `schema.table` names) is a
/// partitioned table.
pub(crate) async fn any_partitioned(
    client: &impl tokio_postgres::GenericClient,
    tables: &[&str],
) -> Result<bool, tokio_postgres::Error> {
    let regclasses: Vec<String> = tables
        .iter()
        .map(|table| super::ddl::regclass_arg(table))
        .collect();
    Ok(client
        .query_one(
            "select exists (select 1 from pg_catalog.pg_class \
                            where oid = any (select pg_catalog.to_regclass(t)::oid \
                                             from unnest($1::text[]) t) \
                              and relkind = 'p')",
            &[&regclasses],
        )
        .await?
        .get(0))
}

/// The floor of definition `id`, whose parsed tree is `def`, computed now
/// over its tables' live column types and stored: wherever Trellis writes
/// the definition or recreates its hidden tables, and where the capture pass
/// sees a column it reads change type and the validator still admits it.
pub(crate) async fn stamp(
    client: &impl tokio_postgres::GenericClient,
    floors: &Floors,
    id: i64,
    def: &TransformDef,
) -> Result<Release, super::catalog::CatalogError> {
    let row = client
        .query_one(
            "select source_table, join_table from transform_definitions where id = $1",
            &[&id],
        )
        .await?;
    let source: String = row.get(0);
    let joined: Option<String> = row.get(1);
    let floor = of_live(client, floors, def, &source, joined.as_deref()).await?;
    store(client, id, floor).await?;
    Ok(floor)
}

/// Stores `floor` as definition `id`'s.
pub(crate) async fn store(
    client: &impl tokio_postgres::GenericClient,
    id: i64,
    floor: Release,
) -> Result<(), tokio_postgres::Error> {
    client
        .execute(
            "update transform_definitions set floor_version = $2 where id = $1",
            &[&id, &floor.to_string()],
        )
        .await?;
    Ok(())
}

/// What a change to the type of a column `def` reads means for it, once the
/// validator has looked at it over the live types, as define would (ADR-0024,
/// "A retype re-stamps or pauses").
#[derive(Debug)]
pub(crate) enum OnRetype {
    /// The validator still admits it: its floor over the new types, to store
    /// in the transaction that records them. It can be lower than before.
    Restamp(Release),
    /// The validator refuses it: it pauses, naming this refusal.
    Pause(super::validate::ValidationError),
}

/// [`OnRetype`] for `def` over the live columns of its source (`from`) and,
/// for a `JOIN`, of its joined table, `partitioned` when a table it reads is.
/// `keys` are the key types the tables' own keys were admitted by
/// ([`live_key_features`]), which the validation over the columns can't see.
pub(crate) fn on_retype(
    floors: &Floors,
    def: &TransformDef,
    partitioned: bool,
    from: &std::collections::HashMap<String, super::validate::JoinColumnInfo>,
    joined: Option<&std::collections::HashMap<String, super::validate::JoinColumnInfo>>,
    keys: &[Feature],
) -> OnRetype {
    match validated_features(def, from, joined) {
        Ok(mut validated) => {
            validated.extend_from_slice(keys);
            OnRetype::Restamp(floors.of(def, partitioned, Some(&validated)))
        }
        Err(refusal) => OnRetype::Pause(refusal),
    }
}

/// The key types of `tables` (qualified `schema.table` names) that admit
/// them as sources, read from the live catalog: the entry that admits each
/// primary-key column ([`super::ddl::admitted_primary_key`], which gates
/// every key space's source and each side of a `JOIN`), and the entry that
/// admits each partition key of a partitioned table
/// ([`super::partition_tree::PartitionTree::key_types`]). Both are admitted
/// away from the validator ([`validated_features`]): they read the catalog
/// across awaits, outside its synchronous collector, so they report their
/// entries instead of recording them.
///
/// `None` when a table's primary key is refused, which define and a resume
/// have already refused it for and a capture pass pauses the retype to it
/// for; the floor then counts at the running release, as a refused
/// validation does.
pub(crate) async fn live_key_features(
    client: &impl tokio_postgres::GenericClient,
    tables: &[&str],
) -> Result<Option<Vec<Feature>>, super::catalog::CatalogError> {
    let mut features = Vec::new();
    for table in tables {
        let admitted_by = match super::ddl::admitted_primary_key(client, table).await {
            Ok((_, admitted_by)) => admitted_by,
            Err(
                super::ddl::DdlError::NoPrimaryKey { .. }
                | super::ddl::DdlError::UnsupportedPrimaryKeyType { .. },
            ) => return Ok(None),
            Err(other) => return Err(super::catalog::CatalogError::Ddl(other)),
        };
        features.extend(admitted_by.into_iter().map(Feature::JoinKey));
        if let Some(tree) = super::partition_tree::read_partition_tree(client, table).await? {
            features.extend(tree.key_types().into_iter().map(Feature::PartitionKeyType));
        }
    }
    Ok(Some(features))
}

/// [`on_retype`] over the live columns of `source` and `joined`, the tables
/// `def` reads.
pub(crate) async fn on_live_retype(
    client: &impl tokio_postgres::GenericClient,
    floors: &Floors,
    def: &TransformDef,
    source: &str,
    joined: Option<&str>,
) -> Result<OnRetype, super::catalog::CatalogError> {
    let tables: Vec<&str> = std::iter::once(source).chain(joined).collect();
    let partitioned = any_partitioned(client, &tables).await?;
    let from = super::catalog::column_infos(client, source).await?;
    let joined_columns = match joined {
        Some(joined) => Some(super::catalog::column_infos(client, joined).await?),
        None => None,
    };
    let Some(keys) = live_key_features(client, &tables).await? else {
        return Ok(
            match validated_features(def, &from, joined_columns.as_ref()) {
                Ok(_) => OnRetype::Restamp(floors.of(def, partitioned, None)),
                Err(refusal) => OnRetype::Pause(refusal),
            },
        );
    };
    Ok(on_retype(
        floors,
        def,
        partitioned,
        &from,
        joined_columns.as_ref(),
        &keys,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::defs::parse;

    fn floors(binary: &str, overrides: &[(&str, &str)]) -> Floors {
        Floors::new(
            binary,
            overrides
                .iter()
                .map(|&(feature, since)| {
                    (feature.to_string(), Release::parse(since).expect("release"))
                })
                .collect(),
        )
    }

    fn floor_of(floors: &Floors, text: &str) -> String {
        floors
            .of(&parse(text).expect("parses"), false, Some(&[]))
            .to_string()
    }

    #[test]
    fn a_release_parses_only_as_major_minor_patch() {
        assert_eq!(Release::parse("0.2.0"), Some(Release::new(0, 2, 0)));
        assert_eq!(Release::parse("10.0.12"), Some(Release::new(10, 0, 12)));
        for text in [
            "",
            "0.2",
            "0.2.0.1",
            "0.2.0-rc.1",
            "v0.2.0",
            "0.02.0",
            "0..0",
            "a.b.c",
        ] {
            assert_eq!(Release::parse(text), None, "{text:?}");
        }
        assert_eq!(Release::new(0, 10, 0).to_string(), "0.10.0");
    }

    #[test]
    fn releases_order_as_semver_orders_them() {
        let order = ["0.2.0", "0.2.1", "0.10.0", "1.0.0"].map(|t| Release::parse(t).unwrap());
        assert!(order.windows(2).all(|pair| pair[0] < pair[1]), "{order:?}");
    }

    #[test]
    fn a_binary_belongs_to_the_release_its_version_starts_with() {
        assert_eq!(Release::of_binary("0.3.0"), Release::parse("0.3.0"));
        assert_eq!(Release::of_binary("0.3.0-rc.1"), Release::parse("0.3.0"));
        assert_eq!(
            Release::of_binary("9.9.9-handover-test"),
            Release::parse("9.9.9")
        );
        assert_eq!(Release::of_binary("1.2.3+build.5"), Release::parse("1.2.3"));
        assert_eq!(Release::of_binary("next"), None);
    }

    /// Every key space and option, and the expressions of each kind, have
    /// floor 0.2.0, whatever the running release.
    #[test]
    fn baseline_shapes_have_floor_0_2_0() {
        let shapes = [
            "TRANSFORM t FROM s SELECT a AS a",
            "TRANSFORM app.t FROM app.s SELECT a + 1 AS b, -a AS c, NOT f AS d WHERE TRUE",
            "TRANSFORM t FROM s GROUP BY g SELECT g AS g, SUM(a) AS total, COUNT(*) AS n",
            "TRANSFORM t FROM s LEFT JOIN u ON s.k = u.k GROUP BY s.g WITHOUT SOURCE INDEX \
             SELECT s.g AS g, COUNT(*) AS n",
            "TRANSFORM t FROM s JOIN u ON s.k = u.k SELECT s.a AS a, u.b AS b",
            "TRANSFORM t FROM s SELECT CASE WHEN a IS NULL THEN NULL ELSE a END AS x, \
             COALESCE(a, 0) AS y, NULLIF(a, 0) AS z, ABS(a) AS w, \
             DATE '2024-01-01' AS d, CAST(i AS bigint) AS c, CAST(NULL AS text) AS n, \
             EXTRACT(year FROM ts) AS e, a IS DISTINCT FROM 1 AS f",
        ];
        for binary in ["0.1.0", "0.2.0", "0.3.0"] {
            let floors = floors(binary, &[]);
            for text in shapes {
                assert_eq!(floor_of(&floors, text), "0.2.0", "{binary}: {text}");
            }
        }
        let def = parse(shapes[0]).unwrap();
        assert_eq!(
            floors("0.3.0", &[]).of(&def, true, Some(&[])).to_string(),
            "0.2.0"
        );
    }

    /// `ADOPT EXISTING TABLE` is a feature of its own: a definition that
    /// writes it carries its release, and the same definition without it
    /// doesn't.
    #[test]
    fn adopt_existing_table_carries_its_release() {
        let later = floors("0.5.0", &[("AdoptTarget", "0.3.0")]);
        assert_eq!(
            floor_of(
                &later,
                "TRANSFORM t ADOPT EXISTING TABLE FROM s SELECT a AS a"
            ),
            "0.3.0"
        );
        assert_eq!(
            floor_of(&later, "TRANSFORM t FROM s SELECT a AS a"),
            "0.2.0"
        );
        assert_eq!(
            floor_of(
                &floors("0.2.0", &[]),
                "TRANSFORM t ADOPT EXISTING TABLE FROM s GROUP BY g SELECT g AS g, COUNT(*) AS n"
            ),
            "0.2.0"
        );
    }

    /// A table alias is a feature of its own: a `JOIN` that aliases either
    /// table carries its release, and the same `JOIN` without aliases
    /// doesn't.
    #[test]
    fn a_table_alias_carries_its_release() {
        let later = floors("0.5.0", &[("TableAlias", "0.3.0")]);
        for aliased in [
            "TRANSFORM t FROM s AS a JOIN u ON a.k = u.k GROUP BY a.g SELECT a.g AS g, \
             COUNT(*) AS n",
            "TRANSFORM t FROM s JOIN app.u b ON s.k = b.k SELECT s.a AS a, b.b AS b",
        ] {
            assert_eq!(floor_of(&later, aliased), "0.3.0", "{aliased}");
        }
        for plain in [
            "TRANSFORM t FROM s JOIN u ON s.k = u.k GROUP BY s.g SELECT s.g AS g, COUNT(*) AS n",
            "TRANSFORM t FROM s JOIN app.u ON s.k = u.k SELECT s.a AS a, u.b AS b",
        ] {
            assert_eq!(floor_of(&later, plain), "0.2.0", "{plain}");
        }
        assert_eq!(
            floor_of(
                &floors("0.2.0", &[]),
                "TRANSFORM t FROM s a JOIN u b ON a.k = b.k SELECT a.a AS a"
            ),
            "0.2.0"
        );
    }

    /// A construct registered at a later release raises the floor of a tree
    /// that uses it, wherever in the tree it sits, and only of such a tree.
    #[test]
    fn a_later_construct_raises_the_floor_of_a_tree_using_it() {
        let later = floors(
            "0.5.0",
            &[
                ("Expr(Case)", "0.3.0"),
                ("Function(\"ABS\")", "0.4.0"),
                ("Operator(Modulo)", "0.3.1"),
                ("KeySpace(JoinOneToOne)", "0.4.1"),
                ("PartitionedSource", "0.3.2"),
            ],
        );
        let cases = [
            ("TRANSFORM t FROM s SELECT a AS a", "0.2.0"),
            (
                "TRANSFORM t FROM s SELECT CASE WHEN f THEN 1 END AS x",
                "0.3.0",
            ),
            (
                "TRANSFORM t FROM s SELECT 1 + COALESCE(ABS(a), 0) AS x",
                "0.4.0",
            ),
            (
                "TRANSFORM t FROM s SELECT CASE WHEN f THEN a % 2 END AS x",
                "0.3.1",
            ),
            (
                "TRANSFORM t FROM s GROUP BY g SELECT g AS g, SUM(ABS(a)) AS x",
                "0.4.0",
            ),
            (
                "TRANSFORM t FROM s JOIN u ON s.k = u.k SELECT s.a AS a",
                "0.4.1",
            ),
        ];
        for (text, floor) in cases {
            assert_eq!(floor_of(&later, text), floor, "{text}");
        }
        let plain = parse("TRANSFORM t FROM s SELECT a AS a").unwrap();
        assert_eq!(later.of(&plain, true, Some(&[])).to_string(), "0.3.2");
    }

    /// An `ALTER` stamps the floor of the edited tree: adding the only
    /// newer construct raises it, removing it lowers it again.
    #[test]
    fn an_edit_raises_and_lowers_the_floor() {
        let later = floors("0.3.0", &[("Expr(Case)", "0.3.0")]);
        let mut def = parse("TRANSFORM t FROM s SELECT a AS a").unwrap();
        assert_eq!(later.of(&def, false, Some(&[])).to_string(), "0.2.0");
        let edited = parse("TRANSFORM t FROM s SELECT CASE WHEN f THEN a END AS b").unwrap();
        def.fields.push(edited.fields[0].clone());
        assert_eq!(later.of(&def, false, Some(&[])).to_string(), "0.3.0");
        def.fields.pop();
        assert_eq!(later.of(&def, false, Some(&[])).to_string(), "0.2.0");
    }

    /// A layout this release writes counts like a grammar construct, so a
    /// resume's rebuild into a newer layout raises the floor.
    #[test]
    fn a_newer_layout_raises_the_floor_of_its_key_space_only() {
        let later = floors("0.3.0", &[("Layout(GroupBasis)", "0.3.0")]);
        assert_eq!(
            floor_of(
                &later,
                "TRANSFORM t FROM s GROUP BY g SELECT g AS g, COUNT(*) AS n"
            ),
            "0.3.0"
        );
        assert_eq!(
            floor_of(&later, "TRANSFORM t FROM s SELECT a AS a"),
            "0.2.0"
        );
    }

    /// The floor never exceeds the running release, and a development
    /// build of a release before 0.2.0 counts as 0.2.0.
    #[test]
    fn the_floor_never_exceeds_the_running_release() {
        let text = "TRANSFORM t FROM s SELECT CASE WHEN f THEN a END AS b";
        assert_eq!(
            floor_of(&floors("0.3.0", &[("Expr(Case)", "0.4.0")]), text),
            "0.3.0"
        );
        assert_eq!(
            floor_of(&floors("0.3.0-rc.1", &[("Expr(Case)", "0.4.0")]), text),
            "0.3.0"
        );
        assert_eq!(
            floor_of(&floors("0.1.0", &[("Expr(Case)", "0.4.0")]), text),
            "0.2.0"
        );
    }

    /// A function the registries don't declare counts at the running
    /// release, never lower.
    #[test]
    fn an_undeclared_function_counts_at_the_running_release() {
        let def = parse("TRANSFORM t FROM s SELECT a AS a").unwrap();
        let mut unknown = def.clone();
        unknown.fields[0].expr = Expr::FunctionCall {
            name: "NOT_A_FUNCTION".to_string(),
            args: vec![],
        };
        assert_eq!(
            floors("0.4.0", &[])
                .of(&unknown, false, Some(&[]))
                .to_string(),
            "0.4.0"
        );
        assert_eq!(
            floors("0.4.0", &[]).of(&def, false, Some(&[])).to_string(),
            "0.2.0"
        );
    }

    /// (definition floor, running version) → drop or refuse (ADR-0024 rule
    /// 5): only a floor above the running release refuses, semver-ordered,
    /// and a stored floor that isn't a release never reads as one the binary
    /// meets.
    #[test]
    fn only_a_floor_above_the_running_release_is_above_it() {
        for (floor, running, above) in [
            ("0.2.0", "0.2.0", false),
            ("0.2.0", "0.3.0", false),
            ("0.3.0", "0.3.0", false),
            ("0.3.0", "0.2.0", true),
            ("0.2.1", "0.2.0", true),
            ("0.10.0", "0.9.0", true),
            ("0.9.0", "0.10.0", false),
            ("1.0.0", "0.99.99", true),
            // A pre-release belongs to its release.
            ("0.3.0", "0.3.0-rc.1", false),
            ("0.3.1", "0.3.0-rc.1", true),
            // A development build below 0.2.0 runs as 0.2.0.
            ("0.2.0", "0.1.0", false),
            ("0.2.1", "0.1.0", true),
            // An unknown stored value is above every release.
            ("0.3", "0.9.0", true),
            ("0.3.0-rc.1", "0.9.0", true),
            ("", "0.9.0", true),
            // A version that isn't a release is above every floor.
            ("9.9.9", "next", false),
            ("0.3", "next", false),
        ] {
            assert_eq!(
                floors(running, &[]).above_running(floor).is_some(),
                above,
                "floor {floor:?}, running {running:?}"
            );
        }
        // The refusal names the release the binary runs as.
        assert_eq!(
            floors("0.1.0", &[]).above_running("0.3.0"),
            Some(Release::new(0, 2, 0))
        );
        assert_eq!(
            floors("0.3.0-rc.1", &[]).above_running("0.4.0"),
            Some(Release::new(0, 3, 0))
        );
    }

    /// (definition floor, grammar version, running release) → run, or pause
    /// with which reason (ADR-0024 rule 3); the same answer is what `RESUME`
    /// refuses with. The floor is asked before the grammar.
    #[test]
    fn a_definition_runs_unless_its_floor_or_grammar_is_newer() {
        const CURRENT: i32 = 1;
        for (floor, grammar, running, expected) in [
            ("0.2.0", 1, "0.2.0", None),
            ("0.2.0", 1, "0.3.0", None),
            ("0.3.0", 1, "0.3.0", None),
            ("0.2.0", 0, "0.2.0", None),
            ("0.3.0", 1, "0.2.0", Some("floor")),
            ("0.3.0", 2, "0.2.0", Some("floor")),
            ("0.3", 1, "0.2.0", Some("floor")),
            ("0.2.0", 2, "0.2.0", Some("grammar")),
            ("0.2.0", 2, "0.3.0", Some("grammar")),
            // A binary whose version isn't a release meets every floor, but
            // still reads only its own grammar.
            ("9.9.9", 1, "next", None),
            ("9.9.9", 2, "next", Some("grammar")),
        ] {
            let decided = floors(running, &[]).unrunnable_in(floor, grammar, CURRENT);
            let kind = decided.as_ref().map(|unrunnable| match unrunnable {
                Unrunnable::AboveFloor { .. } => "floor",
                Unrunnable::NewerGrammar { .. } => "grammar",
                Unrunnable::UnknownValue { .. } => "value",
            });
            assert_eq!(
                kind, expected,
                "floor {floor}, grammar {grammar}, running {running}"
            );
        }
        let above = floors("0.2.0", &[])
            .unrunnable_in("0.3.0", 1, CURRENT)
            .unwrap()
            .reason("public.t");
        assert_eq!(
            above,
            "this release can't run transform public.t: its floor, the oldest Trellis release \
             that can run it, is 0.3.0, and this binary runs release 0.2.0; roll forward to \
             0.3.0 or later, then RESUME it or DROP it there"
        );
        let newer = floors("0.2.0", &[])
            .unrunnable_in("0.2.0", 2, CURRENT)
            .unwrap()
            .reason("public.t");
        assert!(
            newer.contains("grammar version 2") && newer.contains("up to 1"),
            "{newer}"
        );
        assert!(
            newer.ends_with("then RESUME it or DROP it there"),
            "{newer}"
        );
    }

    /// (current `capture_failure`, the pause's reason) → the record's new
    /// error: an unpaused definition gets the reason; a paused one keeps its
    /// reasons after it; a later attach replaces the reason an earlier one
    /// recorded, and one that finds it already recorded changes nothing.
    #[test]
    fn pausing_keeps_earlier_reasons_and_never_adds_its_own_twice() {
        let floors_02 = floors("0.2.0", &[]);
        let at_020 = floors_02.unrunnable("0.3.0", 1).unwrap().reason("public.t");
        let at_021 = floors("0.2.1", &[])
            .unrunnable("0.3.0", 1)
            .unwrap()
            .reason("public.t");
        assert_eq!(unrunnable_error(None, &at_020), Some(at_020.clone()));
        assert_eq!(unrunnable_error(Some(&at_020), &at_020), None);
        assert_eq!(
            unrunnable_error(Some(&at_020), &at_021),
            Some(at_021.clone())
        );

        let operator = "paused by the operator";
        let both = unrunnable_error(Some(operator), &at_020).unwrap();
        assert_eq!(both, format!("{at_020} It was already paused: {operator}"));
        assert_eq!(unrunnable_error(Some(&both), &at_020), None);
        assert_eq!(
            unrunnable_error(Some(&both), &at_021),
            Some(format!("{at_021} It was already paused: {operator}"))
        );
    }

    /// Every name the parser builds a call of is declared somewhere: a
    /// scalar function, an aggregate or a special form.
    #[test]
    fn every_function_name_is_declared() {
        let names = super::super::registry::FUNCTIONS
            .iter()
            .map(|spec| spec.name)
            .chain(super::super::registry::AGGREGATE_FUNCTIONS.iter().copied())
            .chain(SPECIAL_FORMS.iter().map(|&(name, _)| name));
        for name in names {
            assert!(function_since(name).is_some(), "{name}");
            assert_eq!(function_name(name), Some(name));
        }
    }

    /// A name's since is the latest of all its overloads', wherever in the
    /// registry each sits, so an overload a later release adds raises every
    /// call of the name.
    #[test]
    fn a_function_counts_at_its_latest_overload() {
        use super::super::registry::FunctionSpec;
        let overload = |arg, since| FunctionSpec {
            name: "SUM",
            arg_types: std::slice::from_ref(arg),
            return_type: ValueType::Numeric,
            since,
        };
        let specs = [
            overload(&ValueType::Numeric, BASELINE),
            overload(&ValueType::Text, Release::new(0, 3, 0)),
            overload(&ValueType::Boolean, Release::new(0, 2, 5)),
        ];
        assert_eq!(
            latest_overload("SUM", specs.iter()),
            Some(Release::new(0, 3, 0))
        );
        assert_eq!(latest_overload("AVG", specs.iter()), None);
    }

    /// An argument the validator checks, not the parser (an `EXTRACT`
    /// field, a `DATE_TRUNC` unit), is a value in the tree, not a node kind:
    /// text with one this release refuses still parses, which is all
    /// loading a stored definition does. The tree's features can't tell the
    /// two apart; the validation's ([`validated_features`]) count the value.
    #[test]
    fn a_validator_checked_argument_parses_without_a_feature_of_its_own() {
        let columns = std::collections::HashMap::from([(
            "ts".to_string(),
            ValueType::Other(crate::defs::pg_type::PgType::Timestamp),
        )]);
        for (admitted, refused) in [
            ("EXTRACT(year FROM ts) AS x", "EXTRACT(julian FROM ts) AS x"),
            (
                "DATE_TRUNC('month', ts) AS x",
                "DATE_TRUNC('fortnight', ts) AS x",
            ),
        ] {
            let admitted = parse(&format!("TRANSFORM t FROM s SELECT {admitted}")).unwrap();
            let refused = parse(&format!("TRANSFORM t FROM s SELECT {refused}"))
                .expect("loading the stored text parses it");
            assert!(crate::defs::validate(&admitted, &columns).is_ok());
            assert!(crate::defs::validate(&refused, &columns).is_err());
            assert_eq!(
                features_used(&admitted, false),
                features_used(&refused, false)
            );
        }
    }

    /// Every typed literal and `CAST` target has its since.
    #[test]
    fn every_typed_literal_and_cast_target_is_declared() {
        for spec in super::super::typed_literal::TYPED_LITERALS {
            assert_eq!(
                Feature::TypedLiteral(spec.value_type).since(),
                Some(spec.since),
                "{}",
                spec.keyword
            );
        }
        for spec in super::super::registry::CAST_TARGETS
            .iter()
            .chain(super::super::registry::NULL_CAST_TARGETS)
        {
            assert!(Feature::CastTarget(spec.value_type).since().is_some());
        }
    }

    fn column(pg_type: &str, value_type: ValueType) -> super::super::validate::JoinColumnInfo {
        super::super::validate::JoinColumnInfo {
            value_type,
            pg_type: pg_type.to_string(),
            type_oid: 0,
            typmod: -1,
            collation_oid: 0,
            collation: None,
            collation_deterministic: true,
            is_enum: false,
            copy_type: pg_type.to_string(),
            copy_collation: None,
            trellis_target: false,
        }
    }

    /// The floor of `select` from `s(id integer, c <c>, d date, ts
    /// timestamp)`, as define computes it: the tree's features and the
    /// validation's over those types. `None` when the validator refuses it.
    fn validated_floor(floors: &Floors, select: &str, c: (&str, ValueType)) -> Option<String> {
        let def = parse(&format!("TRANSFORM t FROM s {select}")).expect("parses");
        let validated = validated_features(&def, &columns(c), None).ok()?;
        Some(floors.of(&def, false, Some(&validated)).to_string())
    }

    /// The columns of `s(id integer, c <c>, d date, ts timestamp)`.
    fn columns(
        c: (&str, ValueType),
    ) -> std::collections::HashMap<String, super::super::validate::JoinColumnInfo> {
        use crate::defs::pg_type::PgType;
        use crate::integer::IntWidth;
        std::collections::HashMap::from([
            (
                "id".to_string(),
                column("integer", ValueType::Integer(IntWidth::Int4)),
            ),
            ("c".to_string(), column(c.0, c.1)),
            (
                "d".to_string(),
                column("date", ValueType::Other(PgType::Date)),
            ),
            (
                "ts".to_string(),
                column(
                    "timestamp without time zone",
                    ValueType::Other(PgType::Timestamp),
                ),
            ),
        ])
    }

    const INT4: (&str, ValueType) = (
        "integer",
        ValueType::Integer(crate::integer::IntWidth::Int4),
    );
    const INT8: (&str, ValueType) = ("bigint", ValueType::Integer(crate::integer::IntWidth::Int8));

    /// Every value and type rule that exists has floor 0.2.0, so a validated
    /// definition's floor is its tree's.
    #[test]
    fn validated_baseline_shapes_have_floor_0_2_0() {
        let floors = floors("0.3.0", &[]);
        for select in [
            "SELECT id AS id, CAST(c AS bigint) AS x",
            "SELECT id AS id, d + c AS x",
            "SELECT id AS id, c > 5 AS x, COALESCE(c, 0) AS y, -c AS z",
            "SELECT id AS id, EXTRACT(hour FROM ts) AS x, DATE_TRUNC('day', ts) AS y",
            "GROUP BY id SELECT id AS id, SUM(c) AS s, AVG(c) AS a, COUNT(*) AS n",
        ] {
            assert_eq!(
                validated_floor(&floors, select, INT4).as_deref(),
                Some("0.2.0"),
                "{select}"
            );
        }
    }

    /// An argument value declared for a later release raises the floor of a
    /// definition using it, and only of one using it.
    #[test]
    fn a_value_declared_later_raises_the_floor() {
        let floors = floors(
            "0.4.0",
            &[
                (r#"Value(DateTruncUnit("week"))"#, "0.3.0"),
                (r#"Value(ExtractField("isodow"))"#, "0.3.1"),
                (r#"Value(TimeZone("UTC"))"#, "0.3.2"),
            ],
        );
        let floor = |select: &str| validated_floor(&floors, select, INT4).expect("validates");
        assert_eq!(
            floor("SELECT id AS id, DATE_TRUNC('week', ts) AS x"),
            "0.3.0"
        );
        assert_eq!(
            floor("SELECT id AS id, DATE_TRUNC('day', ts) AS x"),
            "0.2.0"
        );
        assert_eq!(
            floor("SELECT id AS id, EXTRACT(isodow FROM d) AS x"),
            "0.3.1"
        );
        assert_eq!(floor("SELECT id AS id, EXTRACT(dow FROM d) AS x"), "0.2.0");
        assert_eq!(
            floor("SELECT id AS id, CAST(ts AS timestamp) AS x"),
            "0.2.0"
        );
    }

    /// A type rule's case declared for a later release raises the floor of a
    /// definition the validator admits by it, so the floor depends on the
    /// columns' types: the same text over a widened column uses another case.
    #[test]
    fn a_type_rule_declared_later_raises_the_floor() {
        let floors = floors(
            "0.4.0",
            &[
                ("TypeRule(Cast(IntegerIdentity))", "0.3.0"),
                ("TypeRule(Unify(Integer))", "0.3.1"),
                ("TypeRule(AggregateAdmission(SumAvgInteger))", "0.3.2"),
            ],
        );
        let cast = "SELECT id AS id, CAST(c AS bigint) AS x";
        assert_eq!(
            validated_floor(&floors, cast, INT4).as_deref(),
            Some("0.2.0")
        );
        assert_eq!(
            validated_floor(&floors, cast, INT8).as_deref(),
            Some("0.3.0")
        );
        // `COALESCE(c, 0)`: `bigint` beside `integer` unifies two widths;
        // `integer` beside `integer` is the same type.
        let coalesce = "SELECT id AS id, COALESCE(c, 0) AS x";
        assert_eq!(
            validated_floor(&floors, coalesce, INT4).as_deref(),
            Some("0.2.0")
        );
        assert_eq!(
            validated_floor(&floors, coalesce, INT8).as_deref(),
            Some("0.3.1")
        );
        assert_eq!(
            validated_floor(&floors, "GROUP BY id SELECT id AS id, SUM(c) AS s", INT4).as_deref(),
            Some("0.3.2")
        );
    }

    /// A `GROUP BY` key type declared for a later release raises the floor of
    /// a definition keyed on it, and only of one keyed on it.
    #[test]
    fn a_group_by_key_type_declared_later_raises_the_floor() {
        use crate::defs::pg_type::PgType;
        let floors = floors(
            "0.4.0",
            &[
                ("TypeRule(GroupByKey(Inet))", "0.3.0"),
                ("TypeRule(GroupByKey(Integer))", "0.3.1"),
            ],
        );
        let group = "GROUP BY c SELECT c AS c, COUNT(*) AS n";
        let inet = ("inet", ValueType::Other(PgType::Inet));
        assert_eq!(
            validated_floor(&floors, group, inet).as_deref(),
            Some("0.3.0")
        );
        assert_eq!(
            validated_floor(&floors, group, INT4).as_deref(),
            Some("0.3.1")
        );
        assert_eq!(
            validated_floor(&floors, group, ("text", ValueType::Text)).as_deref(),
            Some("0.2.0")
        );
        // `real` is no key at all: the validator refuses it.
        assert_eq!(
            validated_floor(
                &floors,
                group,
                ("real", ValueType::Float(crate::float::FloatWidth::Float4))
            ),
            None
        );
    }

    /// The floor of `GROUP BY s.g SELECT ...` over `s JOIN u ON s.k = u.k`,
    /// the key `k` of type `k` on both sides and `g` of type `g` on `s`, as
    /// define computes it.
    fn join_floor(floors: &Floors, k: (&str, ValueType), g: (&str, ValueType)) -> Option<String> {
        use crate::defs::pg_type::PgType;
        let def = parse(
            "TRANSFORM t FROM s JOIN u ON s.k = u.k GROUP BY s.g \
             SELECT s.g AS g, COUNT(*) AS n",
        )
        .expect("parses");
        let side = |columns: &[(&str, (&str, ValueType))]| {
            columns
                .iter()
                .map(|(name, (pg_type, value_type))| {
                    let mut info = column(pg_type, *value_type);
                    info.is_enum = matches!(value_type, ValueType::Other(PgType::Enum(_)));
                    (name.to_string(), info)
                })
                .collect::<std::collections::HashMap<_, _>>()
        };
        let left = side(&[("k", k), ("g", g)]);
        let right = side(&[("k", k)]);
        let validated = validated_features(&def, &left, Some(&right)).ok()?;
        Some(floors.of(&def, false, Some(&validated)).to_string())
    }

    /// A `JOIN ON` key type declared for a later release raises the floor of
    /// a definition joined on it, an enum key included; so does a `GROUP BY`
    /// key of a `JOIN`.
    #[test]
    fn a_join_key_type_declared_later_raises_the_floor() {
        use crate::defs::pg_type::PgType;
        let floors = floors(
            "0.4.0",
            &[
                (r#"JoinKey(Listed("bigint"))"#, "0.3.0"),
                ("JoinKey(Enum)", "0.3.1"),
                ("TypeRule(GroupByKey(Inet))", "0.3.2"),
            ],
        );
        let text = ("text", ValueType::Text);
        assert_eq!(join_floor(&floors, INT8, text).as_deref(), Some("0.3.0"));
        assert_eq!(join_floor(&floors, INT4, text).as_deref(), Some("0.2.0"));
        // A modifier never changes the type: `varchar(20)` is `character varying`.
        assert_eq!(
            join_floor(&floors, ("character varying(20)", ValueType::Text), text).as_deref(),
            Some("0.2.0")
        );
        let inet = ("inet", ValueType::Other(PgType::Inet));
        assert_eq!(join_floor(&floors, INT4, inet).as_deref(), Some("0.3.2"));
        let status = (
            "status",
            ValueType::Other(PgType::Enum("enum:public.status")),
        );
        assert_eq!(join_floor(&floors, status, text).as_deref(), Some("0.3.1"));
    }

    /// The key types the tables' own keys were admitted by count with what the
    /// validator admitted: a 1-1 source's primary key (an enum included) and a
    /// partitioned table's partition key, declared later. Their release is the
    /// entry's, the join-key allowlist's for a primary key.
    #[test]
    fn a_table_key_type_declared_later_raises_the_floor() {
        use crate::defs::join_key_types::JoinKey;
        let floors = floors(
            "0.4.0",
            &[
                (r#"JoinKey(Listed("uuid"))"#, "0.3.0"),
                ("JoinKey(Enum)", "0.3.1"),
                (r#"PartitionKeyType("date")"#, "0.3.2"),
            ],
        );
        let def = parse("TRANSFORM t FROM s SELECT id AS id").unwrap();
        let on =
            |keys: &[Feature]| match on_retype(&floors, &def, false, &columns(INT4), None, keys) {
                OnRetype::Restamp(floor) => floor.to_string(),
                OnRetype::Pause(refusal) => panic!("paused: {refusal}"),
            };
        assert_eq!(on(&[]), "0.2.0");
        assert_eq!(on(&[Feature::JoinKey(JoinKey::Listed("text"))]), "0.2.0");
        assert_eq!(on(&[Feature::JoinKey(JoinKey::Listed("uuid"))]), "0.3.0");
        assert_eq!(on(&[Feature::JoinKey(JoinKey::Enum)]), "0.3.1");
        assert_eq!(on(&[Feature::PartitionKeyType("bigint")]), "0.2.0");
        assert_eq!(on(&[Feature::PartitionKeyType("date")]), "0.3.2");
        // The latest of them, and a key the entry no longer declares counts at
        // the running release.
        assert_eq!(
            on(&[
                Feature::JoinKey(JoinKey::Listed("uuid")),
                Feature::PartitionKeyType("date"),
            ]),
            "0.3.2"
        );
        assert_eq!(on(&[Feature::PartitionKeyType("inet")]), "0.4.0");
    }

    /// The validator's refusal is no floor: `date + bigint` has no operator.
    #[test]
    fn a_refused_definition_has_no_validated_floor() {
        let floors = floors("0.3.0", &[]);
        let select = "SELECT id AS id, d + c AS x";
        assert_eq!(
            validated_floor(&floors, select, INT4).as_deref(),
            Some("0.2.0")
        );
        assert_eq!(validated_floor(&floors, select, INT8), None);
        // Counted at the running release when it is computed anyway.
        let def = parse(&format!("TRANSFORM t FROM s {select}")).unwrap();
        assert_eq!(floors.of(&def, false, None).to_string(), "0.3.0");
    }

    /// A rule called outside a validation records nothing, and a validation
    /// nested in another counts toward both.
    #[test]
    fn only_a_collecting_validation_records() {
        use super::super::registry::{CastRule, TypeRule};
        let int4 = ValueType::Integer(crate::integer::IntWidth::Int4);
        let identity = Feature::TypeRule(TypeRule::Cast(CastRule::IntegerIdentity));
        super::super::registry::cast_result_type(int4, int4);
        let ((_, inner), outer) =
            collect(|| collect(|| super::super::registry::cast_result_type(int4, int4)));
        assert_eq!(inner, vec![identity]);
        assert_eq!(outer, vec![identity]);
        assert_eq!(collect(|| ()).1, Vec::<Feature>::new());
    }

    /// A retype the validator still admits re-stamps, up or down, by the case
    /// that admits the new types; one it refuses pauses with the refusal.
    #[test]
    fn a_retype_restamps_or_pauses() {
        let floors = floors("0.4.0", &[("TypeRule(Cast(IntegerIdentity))", "0.3.0")]);
        let on = |select: &str, c| {
            let def = parse(&format!("TRANSFORM t FROM s {select}")).unwrap();
            on_retype(&floors, &def, false, &columns(c), None, &[])
        };
        let restamped = |select: &str, c| match on(select, c) {
            OnRetype::Restamp(floor) => floor.to_string(),
            OnRetype::Pause(refusal) => panic!("{select}: paused: {refusal}"),
        };
        // `integer` to `bigint` under `CAST(c AS bigint)`: the identity cast
        // now, so the floor goes up; back again, down.
        let cast = "SELECT id AS id, CAST(c AS bigint) AS x";
        assert_eq!(restamped(cast, INT4), "0.2.0");
        assert_eq!(restamped(cast, INT8), "0.3.0");
        // `date + c` takes no `bigint`: the retype pauses, naming why.
        let days = "SELECT id AS id, d + c AS x";
        assert_eq!(restamped(days, INT4), "0.2.0");
        match on(days, INT8) {
            OnRetype::Pause(refusal) => {
                assert!(refusal.to_string().contains("date arithmetic"), "{refusal}")
            }
            OnRetype::Restamp(floor) => panic!("restamped at {floor}"),
        }
        // `smallint` to `bigint` under `CAST(c AS integer)`: a narrowing
        // cast now, which the validator refuses (#1640's accepted cost).
        let narrowing = "SELECT id AS id, CAST(c AS integer) AS x";
        assert_eq!(
            restamped(
                narrowing,
                (
                    "smallint",
                    ValueType::Integer(crate::integer::IntWidth::Int2)
                )
            ),
            "0.2.0"
        );
        assert!(matches!(on(narrowing, INT8), OnRetype::Pause(_)));
    }
}
