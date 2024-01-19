use crate::err::Error;
use crate::sql::value::serde::ser;
use bnum::types::{I512};
use revision::Error as RevisionError;
use revision::Revisioned;
use rust_decimal::prelude::*;
use serde::de::{self, Visitor};
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::{Deserialize, Deserializer, Serialize, Serializer as SerdeSerializer};
use std::fmt::{Display, Formatter};
use std::iter::Product;
use std::iter::Sum;
use std::ops::{Add, Div, Mul, Neg, Rem, Sub};
use std::str::FromStr;

pub(super) struct Serializer;

#[derive(Clone, Debug, Copy, Default, PartialEq, Eq, Hash)]
pub struct BiggerInt(I512);

impl Serialize for BiggerInt {
	fn serialize<S: SerdeSerializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		let mut hex = self.0.to_str_radix(16);
		if hex.starts_with('-') {
			hex = "-0x".to_owned() + &hex[1..];
		} else {
			hex = "0x".to_owned() + &hex;
		}
		serializer.serialize_str(hex.as_str())
	}
}

impl<'de> Deserialize<'de> for BiggerInt {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		deserializer.deserialize_str(BiggerIntVisitor)
	}
}

struct BiggerIntVisitor;

impl<'de> Visitor<'de> for BiggerIntVisitor {
	type Value = BiggerInt;

	fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
		formatter.write_str("BiggerInt")
	}

	fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
	where
		E: de::Error,
	{
		match I512::from_str_radix(v, 16) {
			Ok(v) => Ok(BiggerInt(v)),
			Err(_) => Err(de::Error::custom("BiggerInt")),
		}
	}
}

macro_rules! impl_prim_conversions {
	($($int: ty),*) => {
		$(
			impl From<$int> for BiggerInt {
				fn from(i: $int) -> Self {
					Self(I512::try_from(i).unwrap())
				}
			}
		)*
	};
}

impl_prim_conversions!(i8, i16, i32, i64, isize, u8, u16, u32, u64);

impl From<I512> for BiggerInt {
	fn from(v: I512) -> Self {
		Self(v)
	}
}

impl From<usize> for BiggerInt {
	fn from(v: usize) -> Self {
		Self(I512::from_str(v.to_string().as_str()).unwrap())
	}
}

impl From<i128> for BiggerInt {
	fn from(v: i128) -> Self {
		Self(I512::from_str(v.to_string().as_str()).unwrap())
	}
}

impl From<u128> for BiggerInt {
	fn from(v: u128) -> Self {
		Self(I512::from_str(v.to_string().as_str()).unwrap())
	}
}

impl TryFrom<f64> for BiggerInt {
	// todo: [zyre] add support for f64
	type Error = Error;
	fn try_from(v: f64) -> Result<Self, Self::Error> {
		Err(Error::TryFrom(v.to_string(), "BiggerInt"))
	}
}

impl TryFrom<Decimal> for BiggerInt {
	// todo: [zyre] properly handle conversions
	type Error = Error;
	fn try_from(v: Decimal) -> Result<Self, Self::Error> {
		match v.to_i128() {
			Some(v) => Ok(BiggerInt::from(v)),
			None => Err(Error::TryFrom(v.to_string(), "BiggerInt")),
		}
	}
}

impl TryFrom<&str> for BiggerInt {
	// todo: [zyre] properly handle conversions
	type Error = Error;
	fn try_from(v: &str) -> Result<Self, Self::Error> {
		info!("TryFrom<&str> BiggerInt: {}", v);
		match BiggerInt::from_str(v) {
			Ok(v) => Ok(v),
			Err(_) => Err(Error::TryFrom(v.to_string(), "BiggerInt")),
		}
	}
}

impl TryFrom<String> for BiggerInt {
	// todo: [zyre] properly handle conversions
	type Error = Error;
	fn try_from(v: String) -> Result<Self, Self::Error> {
		info!("TryFrom<String> BiggerInt: {}", v);
		match BiggerInt::from_str(v.as_str()) {
			Ok(v) => Ok(v),
			Err(_) => Err(Error::TryFrom(v.to_string(), "BiggerInt")),
		}
	}
}

impl TryFrom<&[u8]> for BiggerInt {
	type Error = Error;
	fn try_from(v: &[u8]) -> Result<Self, Self::Error> {
		let s = String::from_utf8_lossy(v);
		info!("TryFrom<&[u8]> BiggerInt: {}", s);
		match BiggerInt::from_str(s.as_ref()) {
			Ok(v) => Ok(v),
			Err(_) => Err(Error::TryFrom(s.to_string(), "BiggerInt")),
		}
	}
}

const MIN_I8: &I512 = &I512::parse_str_radix("-128", 10);
const MAX_I8: &I512 = &I512::parse_str_radix("127", 10);
const MAX_U8: &I512 = &I512::parse_str_radix("255", 10);
const MIN_I16: &I512 = &I512::parse_str_radix("-32768", 10);
const MAX_I16: &I512 = &I512::parse_str_radix("32767", 10);
const MAX_U16: &I512 = &I512::parse_str_radix("65535", 10);
const MIN_I32: &I512 = &I512::parse_str_radix("-2147483648", 10);
const MAX_I32: &I512 = &I512::parse_str_radix("2147483647", 10);
const MAX_U32: &I512 = &I512::parse_str_radix("4294967295", 10);
const MIN_I64: &I512 = &I512::parse_str_radix("-9223372036854775808", 10);
const MAX_I64: &I512 = &I512::parse_str_radix("9223372036854775807", 10);
const MAX_U64: &I512 = &I512::parse_str_radix("18446744073709551615", 10);
const MIN_I128: &I512 = &I512::parse_str_radix("-170141183460469231731687303715884105728", 10);
const MAX_I128: &I512 = &I512::parse_str_radix("170141183460469231731687303715884105727", 10);
const MAX_U128: &I512 = &I512::parse_str_radix("340282366920938463463374607431768211455", 10);

impl BiggerInt {
	// Satisfy `try_into_prim` macro
	#[inline]
	pub fn to_i8(self) -> Option<i8> {
		if self.0.le(MAX_I8) && self.0.ge(MIN_I8) {
			let bits = self.0.to_bits();
			let casted: &[i8] = bytemuck::cast_slice(bits.digits());
			Option::from(casted[0])
		} else {
			None
		}
	}
	#[inline]
	pub fn to_i16(self) -> Option<i16> {
		if self.0.le(MAX_I16) && self.0.ge(MIN_I16) {
			let bits = self.0.to_bits();
			let casted: &[i16] = bytemuck::cast_slice(bits.digits());
			Option::from(casted[0])
		} else {
			None
		}
	}
	#[inline]
	pub fn to_i32(self) -> Option<i32> {
		if self.0.le(MAX_I32) && self.0.ge(MIN_I32) {
			let bits = self.0.to_bits();
			let casted: &[i32] = bytemuck::cast_slice(bits.digits());
			Option::from(casted[0])
		} else {
			None
		}
	}
	#[inline]
	pub fn to_i64(self) -> Option<i64> {
		if self.0.le(MAX_I64) && self.0.ge(MIN_I64) {
			let bits = self.0.to_bits();
			let casted: &[i64] = bytemuck::cast_slice(bits.digits());
			Option::from(casted[0])
		} else {
			None
		}
	}
	#[inline]
	pub fn to_i128(self) -> Option<i128> {
		if self.0.le(MAX_I128) && self.0.ge(MIN_I128) {
			let bits = self.0.to_bits();
			let casted: &[i128] = bytemuck::cast_slice(bits.digits());
			Option::from(casted[0])
		} else {
			None
		}
	}
	#[inline]
	pub fn to_u8(self) -> Option<u8> {
		if self.0.le(MAX_U8) {
			let bits = self.0.to_bits();
			let casted: &[u8] = bytemuck::cast_slice(bits.digits());
			Option::from(casted[0])
		} else {
			None
		}
	}
	#[inline]
	pub fn to_u16(self) -> Option<u16> {
		if self.0.le(MAX_U16) {
			let bits = self.0.to_bits();
			let casted: &[u16] = bytemuck::cast_slice(bits.digits());
			Option::from(casted[0])
		} else {
			None
		}
	}
	#[inline]
	pub fn to_u32(self) -> Option<u32> {
		if self.0.le(MAX_U32) {
			let bits = self.0.to_bits();
			let casted: &[u32] = bytemuck::cast_slice(bits.digits());
			Option::from(casted[0])
		} else {
			None
		}
	}
	#[inline]
	pub fn to_u64(self) -> Option<u64> {
		if self.0.le(MAX_U64) {
			let bits = self.0.to_bits();
			let casted: &[u64] = bytemuck::cast_slice(bits.digits());
			Option::from(casted[0])
		} else {
			None
		}
	}
	#[inline]
	pub fn to_u128(self) -> Option<u128> {
		if self.0.le(MAX_U128) {
			let bits = self.0.to_bits();
			let casted: &[u128] = bytemuck::cast_slice(bits.digits());
			Option::from(casted[0])
		} else {
			None
		}
	}
	#[inline]
	pub fn to_f32(self) -> Option<f32> {
		let bits = self.0.to_bits();
		let casted: &[f32] = bytemuck::cast_slice(bits.digits());
		Option::from(casted[0])
	}
	#[inline]
	pub fn to_f64(self) -> Option<f64> {
		let bits = self.0.to_bits();
		let casted: &[f64] = bytemuck::cast_slice(bits.digits());
		Option::from(casted[0])
	}
	#[inline]
	pub fn to_usize(self) -> Option<usize> {
		let bits = self.0.to_bits();
		let casted: &[usize] = bytemuck::cast_slice(bits.digits());
		Option::from(casted[0])
	}

	pub fn from_str(s: &str) -> Result<Self, bnum::errors::ParseIntError> {
		let mut sval = s;
		if sval.starts_with('-') {
			sval = &sval[3..];
		} else {
			sval = &sval[2..];
		}
		let v = I512::from_str_radix(sval, 16)?;
		Ok(BiggerInt(v))
	}

	// Forward arithmetic operations
	#[inline]
	pub fn is_zero(&self) -> bool {
		self.0.is_zero()
	}
	#[inline]
	pub fn is_negative(&self) -> bool {
		self.0.is_negative()
	}
	#[inline]
	pub fn is_positive(&self) -> bool {
		self.0.is_positive()
	}
	#[inline]
	pub fn abs(&self) -> Self {
		BiggerInt(self.0.abs())
	}
	#[inline]
	pub fn pow(&self, exp: u32) -> Self {
		BiggerInt(self.0.pow(exp))
	}
	#[inline]
	pub fn cmp(&self, other: Self) -> std::cmp::Ordering {
		self.0.cmp(&other.0)
	}
	#[inline]
	pub fn eq(&self, other: &Self) -> bool {
		self.0.eq(&other.0)
	}
	#[inline]
	pub fn is_zero_or_positive(&self) -> bool {
		self.0.is_zero() || self.0.is_positive()
	}
	#[inline]
	pub fn is_zero_or_negative(&self) -> bool {
		self.0.is_zero() || self.0.is_negative()
	}
	#[inline]
	pub fn zero() -> Self {
		BiggerInt(I512::ZERO)
	}
	#[inline]
	pub fn one() -> Self {
		BiggerInt(I512::ONE)
	}

	// checked arithmetic
	pub fn checked_add(self, rhs: Self) -> Option<Self> {
		self.0.checked_add(rhs.0).map(BiggerInt)
	}

	pub fn checked_sub(self, rhs: Self) -> Option<Self> {
		self.0.checked_sub(rhs.0).map(BiggerInt)
	}

	pub fn checked_mul(self, rhs: Self) -> Option<Self> {
		self.0.checked_mul(rhs.0).map(BiggerInt)
	}

	pub fn checked_div(self, rhs: Self) -> Option<Self> {
		self.0.checked_div(rhs.0).map(BiggerInt)
	}

	pub fn checked_rem(self, rhs: Self) -> Option<Self> {
		self.0.checked_rem(rhs.0).map(BiggerInt)
	}
}

impl Neg for BiggerInt {
	type Output = Self;
	#[inline]
	fn neg(self) -> Self {
		self.0.overflowing_neg().0.into()
	}
}

impl Add<Self> for BiggerInt {
	type Output = Self;
	#[inline]
	fn add(self, rhs: Self) -> Self {
		self.0.overflowing_add(rhs.0).0.into()
	}
}

impl<'a, 'b> Add<&'b BiggerInt> for &'a BiggerInt {
	type Output = BiggerInt;
	#[inline]
	fn add(self, rhs: &'b BiggerInt) -> BiggerInt {
		self.0.overflowing_add(rhs.0).0.into()
	}
}

impl Sub<Self> for BiggerInt {
	type Output = Self;
	#[inline]
	fn sub(self, rhs: Self) -> Self {
		self.0.overflowing_sub(rhs.0).0.into()
	}
}

impl<'a, 'b> Sub<&'b BiggerInt> for &'a BiggerInt {
	type Output = BiggerInt;
	#[inline]
	fn sub(self, rhs: &'b BiggerInt) -> BiggerInt {
		self.0.overflowing_sub(rhs.0).0.into()
	}
}

impl Mul<Self> for BiggerInt {
	type Output = Self;
	#[inline]
	fn mul(self, rhs: Self) -> Self {
		self.0.mul(rhs.0).into()
	}
}

impl<'a, 'b> Mul<&'b BiggerInt> for &'a BiggerInt {
	type Output = BiggerInt;
	#[inline]
	fn mul(self, rhs: &'b BiggerInt) -> BiggerInt {
		self.0.mul(rhs.0).into()
	}
}

impl Div<Self> for BiggerInt {
	type Output = Self;
	#[inline]
	fn div(self, rhs: Self) -> Self {
		self.0.div(rhs.0).into()
	}
}

impl<'a, 'b> Div<&'b BiggerInt> for &'a BiggerInt {
	type Output = BiggerInt;
	#[inline]
	fn div(self, rhs: &'b BiggerInt) -> BiggerInt {
		self.0.div(rhs.0).into()
	}
}

impl Rem<Self> for BiggerInt {
	type Output = Self;
	#[inline]
	fn rem(self, rhs: Self) -> Self {
		self.0.rem(rhs.0).into()
	}
}

impl Sum<Self> for BiggerInt {
	fn sum<I>(iter: I) -> BiggerInt
	where
		I: Iterator<Item = Self>,
	{
		iter.fold(BiggerInt::zero(), |acc, x| acc + x)
	}
}

impl<'a> Sum<&'a Self> for BiggerInt {
	fn sum<I>(iter: I) -> BiggerInt
	where
		I: Iterator<Item = &'a Self>,
	{
		iter.fold(BiggerInt::zero(), |acc, x| acc + *x)
	}
}

impl Product<Self> for BiggerInt {
	fn product<I>(iter: I) -> Self
	where
		I: Iterator<Item = Self>,
	{
		iter.fold(BiggerInt::one(), |acc, x| acc * x)
	}
}

impl<'a> Product<&'a Self> for BiggerInt {
	fn product<I>(iter: I) -> BiggerInt
	where
		I: Iterator<Item = &'a Self>,
	{
		iter.fold(BiggerInt::one(), |acc, x| acc * *x)
	}
}

impl Display for BiggerInt {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		self.0.fmt(f)
	}
}

fn unsafe_u64_to_u8_slice(slice: &[u64]) -> &[u8] {
	unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, std::mem::size_of_val(slice)) }
}

impl Revisioned for BiggerInt {
	fn revision() -> u16 {
		1
	}
	#[inline]
	fn serialize_revisioned<W: std::io::Write>(&self, w: &mut W) -> Result<(), RevisionError> {
		let limbs = self.0.to_bits();
		let digits = limbs.digits();
		let bytes = unsafe_u64_to_u8_slice(digits);
		w.write_all(bytes).map_err(|e| RevisionError::Io(e.raw_os_error().unwrap_or(0)))
	}
	#[inline]
	fn deserialize_revisioned<R: std::io::Read>(r: &mut R) -> Result<Self, RevisionError> {
		let mut v = [0u8; 64];
		
		r.read_exact(v.as_mut_slice())
			.map_err(|e| RevisionError::Io(e.raw_os_error().unwrap()))?;
		Ok(BiggerInt(I512::from_le_slice(&v).unwrap_or(I512::ZERO)))
	}
}

impl ser::Serializer for Serializer {
	type Ok = BiggerInt;
	type Error = Error;

	type SerializeSeq = Impossible<BiggerInt, Error>;
	type SerializeTuple = Impossible<BiggerInt, Error>;
	type SerializeTupleStruct = Impossible<BiggerInt, Error>;
	type SerializeTupleVariant = Impossible<BiggerInt, Error>;
	type SerializeMap = Impossible<BiggerInt, Error>;
	type SerializeStruct = Impossible<BiggerInt, Error>;
	type SerializeStructVariant = Impossible<BiggerInt, Error>;

	const EXPECTED: &'static str = "a struct `BiggerInt`";

	#[inline]
	fn serialize_str(self, value: &str) -> Result<Self::Ok, Error> {
		BiggerInt::from_str(value).map_err(Error::custom)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;
	use serde::Serialize;

	#[test]
	fn u256() {
		let number = BiggerInt::default();
		let serialized = Serialize::serialize(&number, Serializer.wrap()).unwrap();
		assert_eq!(number, serialized);
	}
}
