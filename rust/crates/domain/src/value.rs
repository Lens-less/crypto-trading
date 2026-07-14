use std::{fmt, str::FromStr};

use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use crate::DomainError;

macro_rules! decimal_value {
    ($name:ident, $validation_error:ident, $is_invalid:expr) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub(crate) Decimal);

        impl $name {
            /// Creates a validated decimal value.
            ///
            /// # Errors
            ///
            /// Returns an error when `value` violates this value type's domain.
            pub fn new(value: Decimal) -> Result<Self, DomainError> {
                if ($is_invalid)(value) {
                    return Err(DomainError::$validation_error(value));
                }
                Ok(Self(value))
            }

            pub const fn as_decimal(self) -> Decimal {
                self.0
            }

            pub const fn into_inner(self) -> Decimal {
                self.0
            }
        }

        impl TryFrom<Decimal> for $name {
            type Error = DomainError;

            fn try_from(value: Decimal) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl From<$name> for Decimal {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl FromStr for $name {
            type Err = DomainError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                let decimal = Decimal::from_str(value)
                    .map_err(|_| DomainError::InvalidDecimal(value.to_owned()))?;
                Self::new(decimal)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.0.to_string())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = deserialize_decimal(deserializer)?;
                Self::new(value).map_err(de::Error::custom)
            }
        }
    };
}

decimal_value!(Price, NonPositivePrice, |value: Decimal| value
    <= Decimal::ZERO);
decimal_value!(Quantity, NegativeQuantity, |value: Decimal| value
    .is_sign_negative());

impl Default for Quantity {
    fn default() -> Self {
        Self(Decimal::ZERO)
    }
}

/// A signed monetary amount, for balances and profit/loss.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Money(Decimal);

impl Money {
    pub const fn new(value: Decimal) -> Self {
        Self(value)
    }

    pub const fn as_decimal(self) -> Decimal {
        self.0
    }

    pub const fn into_inner(self) -> Decimal {
        self.0
    }
}

impl From<Decimal> for Money {
    fn from(value: Decimal) -> Self {
        Self::new(value)
    }
}

impl From<Money> for Decimal {
    fn from(value: Money) -> Self {
        value.0
    }
}

impl FromStr for Money {
    type Err = DomainError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Decimal::from_str(value)
            .map(Self::new)
            .map_err(|_| DomainError::InvalidDecimal(value.to_owned()))
    }
}

impl fmt::Display for Money {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Serialize for Money {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'de> Deserialize<'de> for Money {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserialize_decimal(deserializer).map(Self::new)
    }
}

fn deserialize_decimal<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    // JSON's arbitrary-precision token keeps every supported decimal digit.
    // serde_yaml exposes plain numeric scalars as f64, so legacy YAML remains
    // compatible through its shortest finite decimal; quote high-precision
    // YAML values when their original lexical scale must be preserved.
    rust_decimal::serde::arbitrary_precision::deserialize(deserializer)
}
