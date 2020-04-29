use std::cmp::{Ordering, PartialOrd};

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
}
