use parser::primitive::*;
use nom::IResult::*;
use responses::primitive::*;

/// ApiKey version support tuple.
/// 0: api key, 1: min version, 2: max version
#[derive(Debug,PartialEq)]
pub struct ApiVersion(pub i16, pub i16, pub i16);

#[derive(Debug,PartialEq)]
pub struct ApiVersionsResponse<'a> {
  pub error_code: i16,
  pub api_versions: &'a [ApiVersion],
}

pub fn ser_api_versions_response(r: ApiVersionsResponse, output: &mut Vec<u8>) -> () {
  ser_i16(r.error_code, output);
  ser_i32((r.api_versions.len() as i32) * 3 * 2, output);

  for api_version in r.api_versions {
    ser_i16(api_version.0, output);
    ser_i16(api_version.1, output);
    ser_i16(api_version.2, output);
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use nom::*;
  use nom::IResult::*;

  const API_TEST_VERSION_0_1_0: [ApiVersion; 2] = [
    ApiVersion(0, 1, 2),
    ApiVersion(1, 0, 3),
  ];

  #[test]
  fn ser_api_versions_response_test() {
    let mut v: Vec<u8> = vec![];

    ser_api_versions_response(
      ApiVersionsResponse {
        error_code: 0,
        api_versions: &API_TEST_VERSION_0_1_0,
      }, &mut v);

    assert_eq!(&v[..], &[
      0x00, 0x00, // error_code = 0

      0x00, 0x00, 0x00, 0x0c, // API_TEST_VERSION_0_1_0 size

      0x00, 0x00, // api key = 0
      0x00, 0x01, // min version = 1
      0x00, 0x02, // max version = 2

      0x00, 0x01, // api key = 1
      0x00, 0x00, // min version = 0
      0x00, 0x03, // max version = 3
    ][..]);
  }
}