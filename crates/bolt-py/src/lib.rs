use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

/// A Python module implemented in Rust for high-performance subroutines in Bolt.
#[pymodule]
fn bolt_py(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(split_tokens, m)?)?;
    m.add_function(wrap_pyfunction!(regex_preprocess, m)?)?;
    m.add_function(wrap_pyfunction!(cosine_similarity, m)?)?;
    Ok(())
}

/// Splits a string into basic token-like substrings based on whitespace and punctuation.
#[pyfunction]
fn split_tokens(text: &str) -> PyResult<Vec<String>> {
    let mut tokens = Vec::new();
    let mut current = String::new();

    for c in text.chars() {
        if c.is_whitespace() {
            if !current.is_empty() {
                tokens.push(current.clone());
                current.clear();
            }
        } else if c.is_ascii_punctuation() {
            if !current.is_empty() {
                tokens.push(current.clone());
                current.clear();
            }
            tokens.push(c.to_string());
        } else {
            current.push(c);
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }

    Ok(tokens)
}

/// Sanitizes a string by stripping out control characters, multiple spaces,
/// and basic HTML tags to preprocess agent text inputs.
#[pyfunction]
fn regex_preprocess(text: &str) -> PyResult<String> {
    let mut result = String::with_capacity(text.len());
    let mut in_tag = false;
    let mut prev_was_space = false;

    for c in text.chars() {
        if c == '<' {
            in_tag = true;
            continue;
        }
        if c == '>' {
            in_tag = false;
            continue;
        }
        if in_tag {
            continue;
        }

        // Strip control characters
        if c.is_control() {
            continue;
        }

        // Deduplicate whitespace
        if c.is_whitespace() {
            if !prev_was_space {
                result.push(' ');
                prev_was_space = true;
            }
        } else {
            result.push(c);
            prev_was_space = false;
        }
    }

    Ok(result.trim().to_string())
}

/// Computes the cosine similarity between two float vectors.
#[pyfunction]
fn cosine_similarity(v1: Vec<f64>, v2: Vec<f64>) -> PyResult<f64> {
    if v1.len() != v2.len() {
        return Err(PyValueError::new_err("Vectors must have the same length"));
    }
    if v1.is_empty() {
        return Err(PyValueError::new_err("Vectors cannot be empty"));
    }

    let mut dot_product = 0.0;
    let mut norm_a = 0.0;
    let mut norm_b = 0.0;

    for i in 0..v1.len() {
        dot_product += v1[i] * v2[i];
        norm_a += v1[i].powi(2);
        norm_b += v2[i].powi(2);
    }

    if norm_a == 0.0 || norm_b == 0.0 {
        return Ok(0.0);
    }

    Ok(dot_product / (norm_a.sqrt() * norm_b.sqrt()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_tokens() {
        let text = "Hello, world! This is a test.";
        let tokens = split_tokens(text).unwrap();
        assert_eq!(
            tokens,
            vec!["Hello", ",", "world", "!", "This", "is", "a", "test", "."]
        );
    }

    #[test]
    fn test_regex_preprocess() {
        let html = "<div>Hello \t\n world!   </div>";
        let cleaned = regex_preprocess(html).unwrap();
        assert_eq!(cleaned, "Hello world!");
    }

    #[test]
    fn test_cosine_similarity() {
        let v1 = vec![1.0, 2.0, 3.0];
        let v2 = vec![1.0, 2.0, 3.0];
        let sim = cosine_similarity(v1.clone(), v2).unwrap();
        assert!((sim - 1.0).abs() < 1e-9);

        let v3 = vec![-1.0, -2.0, -3.0];
        let sim2 = cosine_similarity(v1, v3).unwrap();
        assert!((sim2 - (-1.0)).abs() < 1e-9);
    }
}
