//! Task metadata keyed by type, and the registry that names those types for
//! storage outside the process (spec 2.3.17, 2.7.2).

use std::any::{Any, TypeId, type_name};
use std::collections::{BTreeMap, HashMap};
use std::fmt;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

use crate::error::{ConfigError, MetadataError};

/// Name reserved by the library for the W3C trace context of a task.
pub const TRACE_PARENT: &str = "trace_parent";

/// A stored metadata value, cloneable and inspectable behind a trait object.
trait MetaValue: Any + Send + Sync {
    fn clone_box(&self) -> Box<dyn MetaValue>;
    fn as_any(&self) -> &dyn Any;
    fn type_name(&self) -> &'static str;
}

impl<T: Clone + Send + Sync + 'static> MetaValue for T {
    fn clone_box(&self) -> Box<dyn MetaValue> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn type_name(&self) -> &'static str {
        type_name::<T>()
    }
}

impl Clone for Box<dyn MetaValue> {
    fn clone(&self) -> Self {
        (**self).clone_box()
    }
}

/// Metadata of a task: at most one value per type.
///
/// Inside the process values are kept as they are, without encoding. Values
/// read from outside the process stay in their encoded form until a typed
/// lookup asks for them ([`resolve`](Self::resolve)), and names this process
/// does not know are carried along unchanged.
///
/// `Debug` lists type names and stored names, never values: metadata may hold
/// details about recipients or secret keys.
#[derive(Clone, Default)]
pub struct Metadata {
    typed: HashMap<TypeId, Box<dyn MetaValue>>,
    raw: BTreeMap<String, Value>,
}

impl Metadata {
    /// Empty metadata.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Stores a value, replacing any earlier value of the same type.
    pub fn insert<T>(&mut self, value: T)
    where
        T: Clone + Send + Sync + 'static,
    {
        self.typed.insert(TypeId::of::<T>(), Box::new(value));
    }

    /// The in-process value of type `T`. Encoded values are not parsed; use
    /// [`resolve`](Self::resolve) for that.
    #[must_use]
    pub fn get<T: 'static>(&self) -> Option<&T> {
        self.typed
            .get(&TypeId::of::<T>())
            .and_then(|v| (**v).as_any().downcast_ref::<T>())
    }

    /// Removes the in-process value of type `T`.
    pub fn remove<T: 'static>(&mut self) -> Option<T> {
        let boxed = self.typed.remove(&TypeId::of::<T>())?;
        let any: Box<dyn Any> = boxed;
        any.downcast::<T>().ok().map(|b| *b)
    }

    /// Whether an in-process value of type `T` is present.
    #[must_use]
    pub fn contains<T: 'static>(&self) -> bool {
        self.typed.contains_key(&TypeId::of::<T>())
    }

    /// Whether there is no metadata at all, typed or encoded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.typed.is_empty() && self.raw.is_empty()
    }

    /// The value of type `T`: the in-process value if there is one, otherwise
    /// the encoded value stored under the name `registry` gives `T`, parsed.
    ///
    /// Returns `Ok(None)` when neither exists, including when `T` is not in
    /// the registry.
    ///
    /// # Errors
    ///
    /// [`MetadataError::Unparsable`] when the encoded value does not parse as
    /// `T`. No default is substituted.
    pub fn resolve<T>(&self, registry: &MetadataRegistry) -> Result<Option<T>, MetadataError>
    where
        T: Clone + DeserializeOwned + 'static,
    {
        if let Some(value) = self.get::<T>() {
            return Ok(Some(value.clone()));
        }
        let Some(name) = registry.name_of::<T>() else {
            return Ok(None);
        };
        let Some(raw) = self.raw.get(name) else {
            return Ok(None);
        };
        T::deserialize(raw)
            .map(Some)
            .map_err(|e| MetadataError::Unparsable {
                name: name.to_owned(),
                type_name: type_name::<T>(),
                reason: e.to_string(),
            })
    }
}

impl fmt::Debug for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut types: Vec<_> = self.typed.values().map(|v| (**v).type_name()).collect();
        types.sort_unstable();
        f.debug_struct("Metadata")
            .field("types", &types)
            .field("encoded", &self.raw.keys().collect::<Vec<_>>())
            .finish()
    }
}

type EncodeFn = fn(&dyn MetaValue) -> Result<Value, serde_json::Error>;

#[derive(Clone, Copy)]
struct Entry {
    name: &'static str,
    encode: EncodeFn,
}

fn encode_as<T: Serialize + 'static>(value: &dyn MetaValue) -> Result<Value, serde_json::Error> {
    match value.as_any().downcast_ref::<T>() {
        Some(v) => serde_json::to_value(v),
        // The registry looks entries up by `TypeId::of::<T>()`, so a value of
        // another type never reaches this function.
        None => unreachable!("metadata value is not a {}", type_name::<T>()),
    }
}

/// Registry of metadata types: gives each type a stable name and the rule to
/// encode and parse it, so metadata survives storage outside the process.
///
/// Queues that keep tasks only in memory do not need it.
///
/// ```
/// use serde::{Deserialize, Serialize};
/// use taskcraft::{Metadata, MetadataRegistry};
///
/// #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// struct Priority(u8);
///
/// let registry = MetadataRegistry::new().register::<Priority>("report.priority")?;
///
/// let mut meta = Metadata::new();
/// meta.insert(Priority(5));
/// let stored = registry.encode(&meta)?;
/// assert_eq!(stored["report.priority"], 5);
///
/// let read_back = registry.decode(stored);
/// assert_eq!(read_back.resolve::<Priority>(&registry)?, Some(Priority(5)));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Default)]
pub struct MetadataRegistry {
    by_type: HashMap<TypeId, Entry>,
    by_name: HashMap<&'static str, TypeId>,
}

impl MetadataRegistry {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `T` under a stable name.
    ///
    /// # Errors
    ///
    /// [`ConfigError::ReservedMetadataName`] for [`TRACE_PARENT`],
    /// [`ConfigError::DuplicateMetadataName`] when the name is taken, and
    /// [`ConfigError::DuplicateMetadataType`] when `T` is already registered.
    pub fn register<T>(mut self, name: &'static str) -> Result<Self, ConfigError>
    where
        T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    {
        if name == TRACE_PARENT {
            return Err(ConfigError::ReservedMetadataName {
                name: name.to_owned(),
            });
        }
        if self.by_name.contains_key(name) {
            return Err(ConfigError::DuplicateMetadataName {
                name: name.to_owned(),
            });
        }
        let id = TypeId::of::<T>();
        if self.by_type.contains_key(&id) {
            return Err(ConfigError::DuplicateMetadataType {
                type_name: type_name::<T>(),
            });
        }
        self.by_type.insert(
            id,
            Entry {
                name,
                encode: encode_as::<T>,
            },
        );
        self.by_name.insert(name, id);
        Ok(self)
    }

    /// The stable name of `T`, if registered.
    #[must_use]
    pub fn name_of<T: 'static>(&self) -> Option<&'static str> {
        self.by_type.get(&TypeId::of::<T>()).map(|e| e.name)
    }

    /// Whether `name` names metadata: a registered type or the reserved
    /// [`TRACE_PARENT`]. Sources that read metadata from transport headers
    /// take only such names (spec 2.12).
    #[must_use]
    pub fn is_registered(&self, name: &str) -> bool {
        name == TRACE_PARENT || self.by_name.contains_key(name)
    }

    /// Encodes metadata as a JSON object of stable name → value (spec 2.7.2).
    ///
    /// Encoded values carried from outside are written back unchanged unless
    /// an in-process value of the same name replaces them.
    ///
    /// # Errors
    ///
    /// [`MetadataError::UnregisteredType`] when an in-process value has no
    /// registered name, [`MetadataError::Encode`] when serialization fails.
    pub fn encode(&self, metadata: &Metadata) -> Result<Map<String, Value>, MetadataError> {
        let mut out = Map::new();
        for value in metadata.typed.values() {
            let value = &**value;
            let entry = self.by_type.get(&value.as_any().type_id()).ok_or_else(|| {
                MetadataError::UnregisteredType {
                    type_name: value.type_name(),
                }
            })?;
            let encoded = (entry.encode)(value).map_err(|e| MetadataError::Encode {
                name: entry.name.to_owned(),
                reason: e.to_string(),
            })?;
            out.insert(entry.name.to_owned(), encoded);
        }
        for (name, raw) in &metadata.raw {
            if !out.contains_key(name) {
                out.insert(name.clone(), raw.clone());
            }
        }
        Ok(out)
    }

    /// Reads metadata from its encoded form. Nothing is parsed here; values
    /// are parsed on lookup.
    #[must_use]
    pub fn decode(&self, encoded: Map<String, Value>) -> Metadata {
        Metadata {
            typed: HashMap::new(),
            raw: encoded.into_iter().collect(),
        }
    }
}

impl fmt::Debug for MetadataRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut names: Vec<_> = self.by_name.keys().collect();
        names.sort_unstable();
        f.debug_struct("MetadataRegistry")
            .field("names", &names)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde_json::json;

    use super::*;

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Priority(u8);

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Customer {
        region: String,
    }

    fn registry() -> MetadataRegistry {
        MetadataRegistry::new()
            .register::<Priority>("report.priority")
            .unwrap()
            .register::<Customer>("billing.customer")
            .unwrap()
    }

    #[test]
    fn second_insert_of_a_type_wins() {
        let mut meta = Metadata::new();
        meta.insert(Priority(1));
        meta.insert(Priority(2));
        assert_eq!(meta.get::<Priority>(), Some(&Priority(2)));
    }

    #[test]
    fn registry_rejects_duplicates_and_reserved_name() {
        let err = registry().register::<u8>("report.priority").unwrap_err();
        assert_eq!(err.to_string(), "duplicate metadata name: report.priority");

        let err = registry().register::<Priority>("other").unwrap_err();
        assert!(err.to_string().starts_with("duplicate metadata name"));
        assert!(matches!(err, ConfigError::DuplicateMetadataType { .. }));

        let err = MetadataRegistry::new()
            .register::<String>(TRACE_PARENT)
            .unwrap_err();
        assert_eq!(err.to_string(), "reserved metadata name: trace_parent");
    }

    #[test]
    fn registered_names_include_the_reserved_one() {
        let registry = registry();
        assert!(registry.is_registered("report.priority"));
        assert!(registry.is_registered(TRACE_PARENT));
        assert!(!registry.is_registered("content-type"));
    }

    #[test]
    fn round_trip_matches_external_form() {
        let registry = registry();
        let mut meta = Metadata::new();
        meta.insert(Priority(5));
        meta.insert(Customer {
            region: "eu".into(),
        });

        let encoded = registry.encode(&meta).unwrap();
        assert_eq!(
            Value::Object(encoded.clone()),
            json!({ "report.priority": 5, "billing.customer": { "region": "eu" } })
        );

        let back = registry.decode(encoded);
        assert_eq!(
            back.resolve::<Priority>(&registry).unwrap(),
            Some(Priority(5))
        );
        assert_eq!(
            back.resolve::<Customer>(&registry).unwrap(),
            Some(Customer {
                region: "eu".into()
            })
        );
    }

    #[test]
    fn unregistered_type_cannot_be_encoded() {
        let mut meta = Metadata::new();
        meta.insert(7_i64);
        let err = registry().encode(&meta).unwrap_err();
        assert_eq!(
            err,
            MetadataError::UnregisteredType {
                type_name: type_name::<i64>()
            }
        );
    }

    #[test]
    fn unknown_names_survive_re_encoding() {
        let newer = registry();
        let older = MetadataRegistry::new()
            .register::<Priority>("report.priority")
            .unwrap();

        let mut meta = Metadata::new();
        meta.insert(Priority(5));
        meta.insert(Customer {
            region: "eu".into(),
        });
        let from_newer = newer.encode(&meta).unwrap();

        // An older reader that does not know `billing.customer`.
        let mut seen_by_older = older.decode(from_newer.clone());
        assert_eq!(
            seen_by_older.resolve::<Priority>(&older).unwrap(),
            Some(Priority(5))
        );
        seen_by_older.insert(Priority(6));
        let rewritten = older.encode(&seen_by_older).unwrap();

        assert_eq!(
            rewritten["billing.customer"],
            from_newer["billing.customer"]
        );
        assert_eq!(rewritten["report.priority"], 6);
    }

    #[test]
    fn unparsable_value_is_an_error_not_a_default() {
        let registry = registry();
        let mut encoded = Map::new();
        encoded.insert("report.priority".into(), json!("high"));
        let meta = registry.decode(encoded);
        let err = meta.resolve::<Priority>(&registry).unwrap_err();
        assert!(
            matches!(err, MetadataError::Unparsable { ref name, .. } if name == "report.priority")
        );
    }

    #[test]
    fn missing_or_unregistered_resolves_to_none() {
        let meta = registry().decode(Map::new());
        assert_eq!(meta.resolve::<Priority>(&registry()).unwrap(), None);
        assert_eq!(meta.resolve::<u16>(&registry()).unwrap(), None);
    }

    #[test]
    fn debug_never_shows_values() {
        let registry = registry();
        let mut meta = Metadata::new();
        meta.insert(Customer {
            region: "secret-region".into(),
        });
        let mut encoded = registry.encode(&meta).unwrap();
        encoded.insert("unknown".into(), json!("secret-raw"));
        let mut mixed = registry.decode(encoded);
        mixed.insert(Priority(9));
        let shown = format!("{mixed:?}");
        assert!(!shown.contains("secret"), "{shown}");
        assert!(
            shown.contains("Priority") && shown.contains("unknown"),
            "{shown}"
        );
    }

    #[test]
    fn remove_and_contains() {
        let mut meta = Metadata::new();
        meta.insert(Priority(3));
        assert!(meta.contains::<Priority>());
        assert_eq!(meta.remove::<Priority>(), Some(Priority(3)));
        assert!(!meta.contains::<Priority>() && meta.is_empty());
    }
}
