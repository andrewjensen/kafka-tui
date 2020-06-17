use fuzzy_matcher::skim::SkimMatcherV2;
use fuzzy_matcher::FuzzyMatcher;
use std::cmp::{Ordering, PartialOrd};

lazy_static! {
    static ref MATCHER: SkimMatcherV2 = SkimMatcherV2::default();
}

pub fn format_padded(input: &str, length: usize) -> String {
    let input_length = input.chars().count();

    match input_length.partial_cmp(&length).unwrap() {
        Ordering::Equal => input.to_string(),
        Ordering::Less => format!("{:padded_length$}", input, padded_length = length),
        Ordering::Greater => {
            let truncated: String = input.chars().into_iter().take(length - 3).collect();
            format!("{}...", truncated)
        }
    }
}

pub fn fuzzy_match(item_text: &str, search_input: &str) -> bool {
    MATCHER.fuzzy_match(item_text, search_input).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_padded_shorter() {
        let result = format_padded("my.topic", 10);
        assert_eq!(result, "my.topic  ");
    }

    #[test]
    fn test_format_padded_longer() {
        let result = format_padded("my.extra.long.topic", 10);
        assert_eq!(result, "my.extr...");
    }

    #[test]
    fn test_format_padded_equal() {
        let result = format_padded("your.topic", 10);
        assert_eq!(result, "your.topic");
    }

    #[test]
    fn test_fuzzy_match_true() {
        assert_eq!(fuzzy_match("my.cool.topic", "myco"), true);
    }

    #[test]
    fn test_fuzzy_match_false() {
        assert_eq!(fuzzy_match("my.cool.topic", "truck"), false);
    }
}
