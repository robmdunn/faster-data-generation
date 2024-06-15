use fake::{Fake, Faker};
use std::fs;

use serde_json::Value;

// #[cfg(feature = "chrono")]
// use chrono;

use polars::prelude::*;
use rayon::prelude::*;

use fake::faker::address::raw::*;
use fake::faker::automotive::raw::*;
use fake::faker::barcode::raw::*;
use fake::faker::boolean::raw::*;
use fake::faker::company::raw::*;
use fake::faker::creditcard::raw::*;
use fake::faker::currency::raw::*;
use fake::faker::finance::raw::*;
use fake::faker::filesystem::raw::*;
use fake::faker::internet::raw::*;
use fake::faker::job::raw::*;
use fake::faker::job::raw::Title as JobTitle;
use fake::faker::lorem::raw::*;
use fake::faker::name::raw::*;
use fake::faker::number::raw::*;
use fake::faker::phone_number::raw::*;
use fake::locales::*;
#[cfg(feature = "random_color")]
use fake::faker::color::raw::*;
#[cfg(feature = "http")]
use fake::faker::http::raw::*;
#[cfg(feature = "chrono")]
use fake::faker::chrono::raw::*;
#[cfg(feature = "uuid")]
use fake::uuid::*;
#[cfg(feature = "rust_decimal")]
use fake::decimal::*;
#[cfg(feature = "bigdecimal")]
use fake::bigdecimal::*;

pub fn load_json(json_file: &str) -> Result<Value, Box<dyn std::error::Error>> {
    let json_str = fs::read_to_string(json_file)?;
    let json: Value = serde_json::from_str(&json_str)?;
    Ok(json)
}

pub fn generate_from_json(
    json_file: &str,
    no_rows: usize,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let json = load_json(json_file)?;

    let mut columns = Vec::new();

    if let Some(columns_def) = json.get("columns").and_then(|c| c.as_array()) {
        for col_def in columns_def {
            let col_name = col_def
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or_default();
            let col_type = col_def
                .get("type")
                .and_then(|t| t.as_str())
                .unwrap_or_default();

            let series_en = create_series_from_type(col_type, col_name, no_rows, EN, col_def);
            columns.push(series_en);
        }
    }
    Ok(DataFrame::new(columns)?)
}

fn create_series_from_type<L>(
    type_name: &str,
    col_name: &str,
    no_rows: usize,
    locale: L,
    col_def: &Value,
) -> Series
where
    L: Data + Sync + Send + Copy,
{
    match type_name {
        "u64" => {
            let data = (0..no_rows)
                .into_par_iter()
                .map(|_| Faker.fake::<u64>())
                .collect::<Vec<u64>>();
            Series::new(col_name, data)
        }
        "Word" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| Word(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "Sentence" => {
            let (start, end) = get_range_args(col_def, 3, 5);
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| Sentence(locale, start..end).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "Paragraph" => {
            let (start, end) = get_range_args(col_def, 3, 5);
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| Paragraph(locale, start..end).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "FirstName" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| FirstName(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "LastName" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| LastName(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "Title" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| fake::faker::name::raw::Title(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "Suffix" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| Suffix(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "Name" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| Name(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "NameWithTitle" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| NameWithTitle(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "Seniority" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| Seniority(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "Field" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| Field(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "Position" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| Position(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "JobTitle" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| JobTitle(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "Digit" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| Digit(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "NumberWithFormat" => {
            let fmt = get_args_string(col_def, "#.##");
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| NumberWithFormat(locale, &fmt).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "Boolean" => {
            let ratio = get_args_u8(col_def, 50);
            let data: Vec<bool> = (0..no_rows)
                .into_par_iter()
                .map(|_| Boolean(locale, ratio).fake::<bool>())
                .collect();
            Series::new(col_name, data)
        }
        "FreeEmailProvider" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| FreeEmailProvider(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "DomainSuffix" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| DomainSuffix(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "FreeEmail" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| FreeEmail(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "SafeEmail" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| SafeEmail(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "Username" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| Username(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "Password" => {
            let (start, end) = get_range_args(col_def, 8, 20);
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| Password(locale, start..end).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "IPv4" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| IPv4(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "IPv6" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| IPv6(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "IP" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| IP(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "MACAddress" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| MACAddress(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "UserAgent" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| UserAgent(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        #[cfg(feature = "http")]
        "RfcStatusCode" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| RfcStatusCode(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        #[cfg(feature = "http")]
        "ValidStatusCode" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| ValidStatusCode(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        #[cfg(feature = "random_color")]
        "HexColor" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| HexColor(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        #[cfg(feature = "random_color")]
        "RgbColor" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| RgbColor(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        #[cfg(feature = "random_color")]
        "RgbaColor" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| RgbaColor(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        #[cfg(feature = "random_color")]
        "HslColor" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| HslColor(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        #[cfg(feature = "random_color")]
        "HslaColor" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| HslaColor(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        #[cfg(feature = "random_color")]
        "Color" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| Color(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "CompanySuffix" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| CompanySuffix(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "CompanyName" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| CompanyName(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "Buzzword" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| Buzzword(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "BuzzwordMiddle" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| BuzzwordMiddle(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "BuzzwordTail" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| BuzzwordTail(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "CatchPhrase" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| CatchPhase(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "BsVerb" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| BsVerb(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "BsAdj" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| BsAdj(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "BsNoun" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| BsNoun(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "Bs" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| Bs(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "Profession" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| Profession(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "Industry" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| Industry(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "CityPrefix" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| CityPrefix(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "CitySuffix" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| CitySuffix(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "CityName" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| CityName(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "CountryName" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| CountryName(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "CountryCode" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| CountryCode(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "StreetSuffix" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| StreetSuffix(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "StreetName" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| StreetName(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "TimeZone" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| TimeZone(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "StateName" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| StateName(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "StateAbbr" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| StateAbbr(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "SecondaryAddressType" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| SecondaryAddressType(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "SecondaryAddress" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| SecondaryAddress(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "ZipCode" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| ZipCode(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "PostCode" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| PostCode(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "BuildingNumber" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| BuildingNumber(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "Latitude" => {
            let data: Vec<f64> = (0..no_rows)
                .into_par_iter()
                .map(|_| Latitude(locale).fake::<f64>())
                .collect();
            Series::new(col_name, data)
        }
        "Longitude" => {
            let data: Vec<f64> = (0..no_rows)
                .into_par_iter()
                .map(|_| Longitude(locale).fake::<f64>())
                .collect();
            Series::new(col_name, data)
        }
        "Geohash" => {
            let precision = get_args_u8(col_def, 11);
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| Geohash(locale, precision).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "LicencePlate" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| LicencePlate(FR_FR).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "Isbn" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| Isbn(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "Isbn13" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| Isbn13(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "Isbn10" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| Isbn10(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "PhoneNumber" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| PhoneNumber(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "CellNumber" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| CellNumber(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        #[cfg(feature = "chrono")]
        "Time" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| Time(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        #[cfg(feature = "chrono")]
        "Date" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| Date(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        #[cfg(feature = "chrono")]
        "DateTime" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| DateTime(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        #[cfg(feature = "chrono")]
        "Duration" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    let duration = Duration(locale).fake::<chrono::Duration>();
                    format!("{}", duration)
                })
                .collect();
            Series::new(col_name, data)
        }
        #[cfg(feature = "chrono")]
        "DateTimeBefore" => {
            let dt = get_args_datetime(col_def, &chrono::Utc::now());
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| DateTimeBefore(locale, dt).fake::<chrono::DateTime<chrono::Utc>>().to_rfc3339())
                .collect();
            Series::new(col_name, data)
        }
        #[cfg(feature = "chrono")]
        "DateTimeAfter" => {
            let dt = get_args_datetime(col_def, &chrono::Utc::now());
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| DateTimeAfter(locale, dt).fake::<chrono::DateTime<chrono::Utc>>().to_rfc3339())
                .collect();
            Series::new(col_name, data)
        }
        #[cfg(feature = "chrono")]
        "DateTimeBetween" => {
            let (start, end) = get_args_datetimerange(col_def);
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| DateTimeBetween(locale, start, end).fake::<chrono::DateTime<chrono::Utc>>().to_rfc3339())
                .collect();
            Series::new(col_name, data)
        }
        "FilePath" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| FilePath(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "FileName" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| FileName(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "FileExtension" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| FileExtension(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "DirPath" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| DirPath(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "Bic" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| Bic(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        #[cfg(feature = "uuid")]
        "UUIDv1" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| UUIDv1.fake::<uuid::Uuid>().to_string())
                .collect();
            Series::new(col_name, data)
        }
        #[cfg(feature = "uuid")]
        "UUIDv3" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| UUIDv3.fake::<uuid::Uuid>().to_string())
                .collect();
            Series::new(col_name, data)
        }
        #[cfg(feature = "uuid")]
        "UUIDv4" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| UUIDv4.fake::<uuid::Uuid>().to_string())
                .collect();
            Series::new(col_name, data)
        }
        #[cfg(feature = "uuid")]
        "UUIDv5" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| UUIDv5.fake::<uuid::Uuid>().to_string())
                .collect();
            Series::new(col_name, data)
        }
        "CurrencyCode" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| CurrencyCode(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "CurrencyName" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| CurrencyName(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "CurrencySymbol" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| CurrencySymbol(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "CreditCardNumber" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| CreditCardNumber(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        #[cfg(feature = "rust_decimal")]
        #[cfg(feature = "rust_decimal")]
        "Decimal" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| Decimal.fake::<rust_decimal::Decimal>().to_string())
                .collect();
            Series::new(col_name, data)
        }
        #[cfg(feature = "rust_decimal")]
        "PositiveDecimal" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| PositiveDecimal.fake::<rust_decimal::Decimal>().to_string())
                .collect();
            Series::new(col_name, data)
        }
        #[cfg(feature = "rust_decimal")]
        "NegativeDecimal" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| NegativeDecimal.fake::<rust_decimal::Decimal>().to_string())
                .collect();
            Series::new(col_name, data)
        }
        #[cfg(feature = "rust_decimal")]
        "NoDecimalPoints" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| NoDecimalPoints.fake::<rust_decimal::Decimal>().to_string())
                .collect();
            Series::new(col_name, data)
        }
        #[cfg(feature = "bigdecimal")]
        "BigDecimal" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| BigDecimal.fake::<bigdecimal::BigDecimal>().to_string())
                .collect();
            Series::new(col_name, data)
        }
        #[cfg(feature = "bigdecimal")]
        "PositiveBigDecimal" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| PositiveBigDecimal.fake::<bigdecimal::BigDecimal>().to_string())
                .collect();
            Series::new(col_name, data)
        }
        #[cfg(feature = "bigdecimal")]
        "NegativeBigDecimal" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| NegativeBigDecimal.fake::<bigdecimal::BigDecimal>().to_string())
                .collect();
            Series::new(col_name, data)
        }
        #[cfg(feature = "bigdecimal")]
        "NoBigDecimalPoints" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| NoBigDecimalPoints.fake::<bigdecimal::BigDecimal>().to_string())
                .collect();
            Series::new(col_name, data)
        }
        _ => panic!("Unsupported type: {}", type_name),
    }
}

fn get_range_args(col_def: &Value, default_start: usize, default_end: usize) -> (usize, usize) {
    if let Some(args) = col_def.get("args").and_then(|a| a.get("range")) {
        if let (Some(start), Some(end)) = (
            args.get("start").and_then(|s| s.as_u64().map(|v| v as usize)),
            args.get("end").and_then(|e| e.as_u64().map(|v| v as usize)),
        ) {
            return (start, end);
        }
    }
    (default_start, default_end)
}

fn get_args_string(col_def: &Value, default: &str) -> String {
    if let Some(args) = col_def.get("args") {
        if let Some(fmt) = args.get("fmt").and_then(|f| f.as_str()) {
            return fmt.to_string();
        }
    }
    default.to_string()
}

fn get_args_u8(col_def: &Value, default: u8) -> u8 {
    if let Some(args) = col_def.get("args") {
        if let Some(ratio) = args.get("ratio").and_then(|r| r.as_u64().map(|v| v as u8)) {
            return ratio;
        }
        if let Some(precision) = args.get("precision").and_then(|p| p.as_u64().map(|v| v as u8)) {
            return precision;
        }
    }
    default
}

fn get_args_datetime(col_def: &Value, default: &chrono::DateTime<chrono::Utc>) -> chrono::DateTime<chrono::Utc> {
    if let Some(args) = col_def.get("args") {
        if let Some(dt) = args.get("dt").and_then(|d| d.as_str()) {
            return chrono::DateTime::parse_from_rfc3339(dt)
                .unwrap_or_else(|_| (*default).into())
                .with_timezone(&chrono::Utc);
        }
    }
    *default
}

fn get_args_datetimerange(
    col_def: &Value,
) -> (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>) {
    let default_start = chrono::DateTime::parse_from_rfc3339("2023-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let default_end = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);

    if let Some(args) = col_def.get("args") {
        if let (Some(start), Some(end)) = (
            args.get("start").and_then(|s| s.as_str()),
            args.get("end").and_then(|e| e.as_str()),
        ) {
            let start_dt = chrono::DateTime::parse_from_rfc3339(start)
                .unwrap_or_else(|_| default_start.into())
                .with_timezone(&chrono::Utc);
            let end_dt = chrono::DateTime::parse_from_rfc3339(end)
                .unwrap_or_else(|_| default_end.into())
                .with_timezone(&chrono::Utc);
            return (start_dt, end_dt);
        }
    }
    (default_start, default_end)
}
