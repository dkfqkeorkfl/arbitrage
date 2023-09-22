use anyhow::{Ok, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{ops, str::FromStr, sync::Arc};
type Ftype = i128;

pub trait Precision<const X: u8> {
    fn get_value() -> u8 {
        X
    }

    fn get_10square() -> Ftype {
        return Ftype::pow(10, X as u32);
    }
}

#[derive(PartialEq, PartialOrd)]
pub struct Precision8;
impl Precision<8> for Precision8 {}

pub const PRECISION8: Precision8 = Precision8 {};

#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct Float<const X: u8, T: Precision<X>> {
    value: Ftype,
    pdata: std::marker::PhantomData<T>,
}

impl<const X: u8, T: Precision<X>> Float<X, T> {
    pub fn from_str(v: &str, p: T) -> Result<Self> {
        let re = regex::Regex::new(r"(e|E)").unwrap();
        if let Some(m) = re.find(v) {
            let origin = Float::from_str(&v[0..m.start()], p)?;
            let point = v.chars().nth(m.start() + 1);
            if None == point {
                return Err(anyhow::anyhow!(
                    "invalid point placed: digits({}) len({})",
                    m.start() + 1,
                    v.len()
                ));
            }

            if point.unwrap() == '-' {
                let y = (&v[m.start() + 2..]).parse::<u32>()?;
                let additional =
                    Float::from_detail(T::get_10square() / (10 as Ftype).pow(y), origin.pdata);
                return Ok(&origin * &additional);
            } else {
                let c = if point.unwrap() == '+' { 2 } else { 1 };
                let y = (&v[m.start() + c..]).parse::<u32>()?;
                let additional =
                    Float::from_detail((10 as Ftype).pow(y + T::get_value() as u32), origin.pdata);
                return Ok(&origin * &additional);
            }
        }

        match v.find(".") {
            Some(i) => {
                if i + 1 >= v.len() {
                    return Err(anyhow::anyhow!(
                        "invalid point placed: digits({}) len({})",
                        i,
                        v.len()
                    ));
                }

                let start = i + 1;
                let precision = std::cmp::min(v.len() - start, T::get_value() as usize);
                let slow = &v[start..start + precision];
                let mut low = slow.parse::<Ftype>()?;
                low *= 10i128.pow(T::get_value() as u32 - precision as u32);

                let shigh = &v[0..i];
                let high = shigh.parse::<Ftype>()?;
                return Ok(Float {
                    pdata: std::marker::PhantomData::<T> {},
                    value: (T::get_10square() * high) + low,
                });
            }
            None => {
                let l = v.parse::<Ftype>()?;
                return Ok(Float {
                    pdata: std::marker::PhantomData::<T> {},
                    value: l * T::get_10square(),
                });
            }
        };
    }

    pub fn ceil(&self) -> Self {
        let h = self.value / T::get_10square();
        let l = self.value % T::get_10square();
        let v = (h + if l > 0 { 1 } else { 0 }) * T::get_10square();

        return Self::from_detail(v, self.pdata);
    }

    pub fn floor(&self) -> Self {
        let h = self.value / T::get_10square();
        return Self::from_detail(h * T::get_10square(), self.pdata);
    }

    pub fn fixed(&self, f: u8) -> Self {
        let n = T::get_10square() / Ftype::pow(10, f as u32);
        return Self::from_detail(self.value / n * n, self.pdata);
    }

    pub fn from_int(v: i128, p: T) -> Self {
        return Float {
            value: v * T::get_10square(),
            pdata: std::marker::PhantomData::<T> {},
        };
    }

    pub fn from_detail(v: Ftype, p: std::marker::PhantomData<T>) -> Self {
        Float { value: v, pdata: p }
    }

    pub fn to_string(&self) -> String {
        let high = self.value / T::get_10square();
        let mut low = self.value % T::get_10square();

        let str = if low == 0 {
            high.to_string()
        } else {
            let low_digit = (0..).take_while(|i| 10i128.pow(*i) <= low).count();
            loop {
                if low % 10 == 0 {
                    low /= 10;
                } else {
                    break;
                }
            }
            format!(
                "{}.{}{}",
                high.to_string(),
                "0".repeat(T::get_value() as usize - low_digit),
                low.to_string()
            )
        };
        return str;
    }
}

impl<'a, 'b, const X: u8, T: Precision<{ X }>> ops::Add<&'b Float<X, T>> for &'a Float<X, T> {
    type Output = Float<X, T>;
    fn add(self, other: &'b Float<X, T>) -> Float<X, T> {
        return Float::from_detail(self.value + other.value, self.pdata);
    }
}

impl<'a, 'b, const X: u8, T: Precision<{ X }>> ops::Mul<&'b Float<X, T>> for &'a Float<X, T> {
    type Output = Float<X, T>;
    fn mul(self, other: &'b Float<X, T>) -> Float<X, T> {
        return Float::from_detail(self.value * other.value / T::get_10square(), self.pdata);
    }
}

impl<'a, 'b, const X: u8, T: Precision<{ X }>> ops::Div<&'b Float<X, T>> for &'a Float<X, T> {
    type Output = Float<X, T>;
    fn div(self, other: &'b Float<X, T>) -> Float<X, T> {
        return Float::from_detail(self.value / other.value, self.pdata);
    }
}

impl<'a, 'b, const X: u8, T: Precision<{ X }>> ops::Sub<&'b Float<X, T>> for &'a Float<X, T> {
    type Output = Float<X, T>;
    fn sub(self, other: &'b Float<X, T>) -> Float<X, T> {
        return Float::from_detail(self.value - other.value, self.pdata);
    }
}

pub type Float8 = Float<8, Precision8>;

#[macro_export]
macro_rules! p8_fromstr {
    ( $x:expr ) => {{
        use crate::opk::float::Float;
        use crate::opk::float::PRECISION8;
        Float::from_str($x, PRECISION8)
    }};
}
#[macro_export]
macro_rules! p8_fromint {
    ( $x:expr ) => {{
        use crate::opk::float::Float;
        use crate::opk::float::PRECISION8;
        Float::from_int($x, PRECISION8)
    }};
}

pub fn to_decimal(s: &str) -> Result<rust_decimal::Decimal> {
    let re = regex::Regex::new(r"(e|E)").unwrap();
    if re.is_match(s) {
        let proxy = p8_fromstr!(s)?;
        let ret = rust_decimal::Decimal::from_str(&proxy.to_string())?;
        return Ok(ret);
    }
    let ret = rust_decimal::Decimal::from_str(s)?;
    return Ok(ret);
}

pub fn to_decimal_with_json(v: &serde_json::Value) -> Result<rust_decimal::Decimal> {
    if v.is_string() {
        to_decimal(v.as_str().unwrap())
    } else {
        to_decimal(&v.to_string())
    }
}

// #[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum LazyDecimal {
    Str(String),
    Num(Decimal),
}

impl Default for LazyDecimal {
    fn default() -> Self {
        LazyDecimal::Num(Decimal::ZERO)
    }
}

impl LazyDecimal {
    pub fn get_num(&self) -> Option<&Decimal> {
        if let LazyDecimal::Num(s) = self {
            Some(s)
        } else {
            None
        }
    }

    pub fn transf_num(&mut self) -> &Decimal {
        if let LazyDecimal::Str(s) = self {
            let n = to_decimal(s).unwrap_or(Decimal::ZERO);
            *self = LazyDecimal::Num(n);
        }

        if let LazyDecimal::Num(s) = self {
            s
        } else {
            &Decimal::ZERO
        }
    }

    pub fn transf_str(&mut self) -> &str {
        if let LazyDecimal::Num(n) = self {
            let s = n.to_string();
            *self = LazyDecimal::Str(s);
        }

        if let LazyDecimal::Str(s) = self {
            s.as_str()
        } else {
            ""
        }
    }

    pub fn to_num(&self) -> Result<rust_decimal::Decimal> {
        match self {
            LazyDecimal::Str(s) => {
                let n = to_decimal(s)?;
                Ok(n)
            }
            LazyDecimal::Num(n) => Ok(n.clone()),
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            LazyDecimal::Str(s) => s.clone(),
            LazyDecimal::Num(n) => n.to_string(),
        }
    }
}
