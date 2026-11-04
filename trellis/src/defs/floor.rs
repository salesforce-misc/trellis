//! Definition floors (ADR-0024): the oldest Trellis release that can run a
//! definition.
//!
//! Every stored definition records its floor in
//! `transform_definitions.floor_version`. It is the highest release any
//! [`Feature`] the definition uses arrived in: the grammar constructs of its
//! parsed tree, its key space and options, whether a table it reads is
//! partitioned, and the hidden-table layout and per-definition state this
//! release writes for it. [`Floors::of`] computes it, and [`stamp`] writes
//! it wherever Trellis writes the definition (define, `ALTER TRANSFORM`) or
//! recreates its hidden tables (a resume's rebuild). A pause or a read
//! never changes it, and it can go down: an `ALTER` that removes the only
//! newer feature lowers it.
//!
//! The registry is exhaustive by construction. [`features_used`] matches
//! every node kind of the tree, and [`Feature::since`] matches every
//! feature, so a new node kind, key space, option or layout can't compile
//! without declaring the release it arrived in. A function, typed literal or
//! `CAST` target declares it in its own registry entry
//! ([`super::registry::FunctionSpec::since`],
//! [`super::typed_literal::TypedLiteralSpec::since`],
//! [`super::registry::CastTargetSpec::since`]). Everything that exists when
//! floors were introduced has floor [`BASELINE`].
//!
//! Acting on a floor is not here: a definition above the running release is
//! paused without being parsed, and `DROP` refuses it (ADR-0024 rules 3 and
//! 5). Both read the stored column, and [`Floors::above_running`] decides
//! whether it is above the running release.

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
        right: _,
        explicit_right_schema,
        on,
        group_by: _,
    }) = join
    {
        used.add(Feature::JoinKind(*kind));
        if explicit_right_schema.is_some() {
            used.add(Feature::QualifiedTable);
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

    /// The floors of this build, with no overrides.
    #[cfg(any(test, feature = "internals"))]
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
    ///
    /// A binary only writes what it carries, so a floor above the running
    /// release is a registry entry declared for a later release than the
    /// binary's; capping it keeps the binary from writing a definition it
    /// would itself refuse to run. A feature no entry declares counts at the
    /// running release, never lower.
    pub(crate) fn of(&self, def: &TransformDef, partitioned: bool) -> Release {
        let (features, unknown) = features_used(def, partitioned);
        let ceiling = self
            .running
            .unwrap_or(Release::new(u64::MAX, u64::MAX, u64::MAX));
        let unknown = unknown.then_some(ceiling);
        features
            .iter()
            .map(|feature| self.since(feature).unwrap_or(ceiling))
            .chain(unknown)
            .max()
            .unwrap_or(BASELINE)
            .min(ceiling)
    }
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
/// and stored: wherever Trellis writes the definition or recreates its
/// hidden tables.
pub(crate) async fn stamp(
    client: &impl tokio_postgres::GenericClient,
    floors: &Floors,
    id: i64,
    def: &TransformDef,
) -> Result<Release, tokio_postgres::Error> {
    let row = client
        .query_one(
            "select source_table, join_table from transform_definitions where id = $1",
            &[&id],
        )
        .await?;
    let source: String = row.get(0);
    let joined: Option<String> = row.get(1);
    let tables: Vec<&str> = std::iter::once(source.as_str())
        .chain(joined.as_deref())
        .collect();
    let floor = floors.of(def, any_partitioned(client, &tables).await?);
    client
        .execute(
            "update transform_definitions set floor_version = $2 where id = $1",
            &[&id, &floor.to_string()],
        )
        .await?;
    Ok(floor)
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
        floors.of(&parse(text).expect("parses"), false).to_string()
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
        assert_eq!(floors("0.3.0", &[]).of(&def, true).to_string(), "0.2.0");
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
        assert_eq!(later.of(&plain, true).to_string(), "0.3.2");
    }

    /// An `ALTER` stamps the floor of the edited tree: adding the only
    /// newer construct raises it, removing it lowers it again.
    #[test]
    fn an_edit_raises_and_lowers_the_floor() {
        let later = floors("0.3.0", &[("Expr(Case)", "0.3.0")]);
        let mut def = parse("TRANSFORM t FROM s SELECT a AS a").unwrap();
        assert_eq!(later.of(&def, false).to_string(), "0.2.0");
        let edited = parse("TRANSFORM t FROM s SELECT CASE WHEN f THEN a END AS b").unwrap();
        def.fields.push(edited.fields[0].clone());
        assert_eq!(later.of(&def, false).to_string(), "0.3.0");
        def.fields.pop();
        assert_eq!(later.of(&def, false).to_string(), "0.2.0");
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
            floors("0.4.0", &[]).of(&unknown, false).to_string(),
            "0.4.0"
        );
        assert_eq!(floors("0.4.0", &[]).of(&def, false).to_string(), "0.2.0");
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
    /// loading a stored definition does, so the floor can't stand in for
    /// it (ADR-0024, "Only the tree is counted").
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
}
