//! Minimal strict JSON reader (mirrors Python `json.loads` acceptance) plus output helpers.

pub enum Field {
    Missing,
    /// Decoded string; `false` when it contained a lone UTF-16 surrogate.
    Str(String, bool),
    /// JSON integer literal (no fraction/exponent) that fits in i64.
    Int(i64),
    Other,
}

struct P<'a> {
    b: &'a [u8],
    i: usize,
}

const MAX_DEPTH: u32 = 512;

impl<'a> P<'a> {
    #[inline]
    fn ws(&mut self) {
        while self.i < self.b.len() && matches!(self.b[self.i], b' ' | b'\t' | b'\n' | b'\r') {
            self.i += 1;
        }
    }
    #[inline]
    fn peek(&self) -> Option<u8> {
        self.b.get(self.i).copied()
    }
    fn lit(&mut self, s: &[u8]) -> Result<(), ()> {
        if self.b[self.i..].starts_with(s) {
            self.i += s.len();
            Ok(())
        } else {
            Err(())
        }
    }
    fn hex4(&mut self) -> Result<u32, ()> {
        if self.i + 4 > self.b.len() {
            return Err(());
        }
        let mut v = 0u32;
        for k in 0..4 {
            let c = self.b[self.i + k];
            let d = match c {
                b'0'..=b'9' => c - b'0',
                b'a'..=b'f' => c - b'a' + 10,
                b'A'..=b'F' => c - b'A' + 10,
                _ => return Err(()),
            } as u32;
            v = v * 16 + d;
        }
        self.i += 4;
        Ok(v)
    }

    /// Cursor at opening quote. Returns Ok(unicode_ok).
    fn string(&mut self, mut out: Option<&mut String>) -> Result<bool, ()> {
        self.i += 1;
        let mut ok = true;
        let mut seg = self.i;
        loop {
            let c = *self.b.get(self.i).ok_or(())?;
            match c {
                b'"' => {
                    if let Some(o) = out.as_deref_mut() {
                        o.push_str(std::str::from_utf8(&self.b[seg..self.i]).map_err(|_| ())?);
                    }
                    self.i += 1;
                    return Ok(ok);
                }
                b'\\' => {
                    if let Some(o) = out.as_deref_mut() {
                        o.push_str(std::str::from_utf8(&self.b[seg..self.i]).map_err(|_| ())?);
                    }
                    self.i += 1;
                    let e = *self.b.get(self.i).ok_or(())?;
                    self.i += 1;
                    let ch: Option<char> = match e {
                        b'"' => Some('"'),
                        b'\\' => Some('\\'),
                        b'/' => Some('/'),
                        b'b' => Some('\u{8}'),
                        b'f' => Some('\u{c}'),
                        b'n' => Some('\n'),
                        b'r' => Some('\r'),
                        b't' => Some('\t'),
                        b'u' => {
                            let cp = self.hex4()?;
                            if (0xD800..0xDC00).contains(&cp) {
                                let save = self.i;
                                if self.b[self.i..].starts_with(b"\\u") {
                                    self.i += 2;
                                    let lo = self.hex4()?;
                                    if (0xDC00..0xE000).contains(&lo) {
                                        char::from_u32(0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00))
                                    } else {
                                        self.i = save;
                                        ok = false;
                                        None
                                    }
                                } else {
                                    ok = false;
                                    None
                                }
                            } else if (0xDC00..0xE000).contains(&cp) {
                                ok = false;
                                None
                            } else {
                                char::from_u32(cp)
                            }
                        }
                        _ => return Err(()),
                    };
                    if let (Some(o), Some(ch)) = (out.as_deref_mut(), ch) {
                        o.push(ch);
                    }
                    seg = self.i;
                }
                0..=0x1f => return Err(()),
                _ => self.i += 1,
            }
        }
    }

    /// Returns Some(n) for an in-range integer literal, None for other valid numbers.
    fn number(&mut self) -> Result<Option<i64>, ()> {
        let start = self.i;
        if self.peek() == Some(b'-') {
            self.i += 1;
        }
        match self.peek() {
            Some(b'0') => self.i += 1,
            Some(b'1'..=b'9') => {
                while matches!(self.peek(), Some(b'0'..=b'9')) {
                    self.i += 1;
                }
            }
            _ => return Err(()),
        }
        let mut int = true;
        if self.peek() == Some(b'.') {
            self.i += 1;
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return Err(());
            }
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.i += 1;
            }
            int = false;
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.i += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.i += 1;
            }
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return Err(());
            }
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.i += 1;
            }
            int = false;
        }
        if !int {
            return Ok(None);
        }
        let s = std::str::from_utf8(&self.b[start..self.i]).map_err(|_| ())?;
        Ok(s.parse::<i64>().ok())
    }

    fn value(&mut self, depth: u32, want: bool) -> Result<Field, ()> {
        if depth > MAX_DEPTH {
            return Err(());
        }
        self.ws();
        match self.peek().ok_or(())? {
            b'{' => {
                self.i += 1;
                self.ws();
                if self.peek() == Some(b'}') {
                    self.i += 1;
                    return Ok(Field::Other);
                }
                loop {
                    self.ws();
                    if self.peek() != Some(b'"') {
                        return Err(());
                    }
                    self.string(None)?;
                    self.ws();
                    if self.peek() != Some(b':') {
                        return Err(());
                    }
                    self.i += 1;
                    self.value(depth + 1, false)?;
                    self.ws();
                    match self.peek() {
                        Some(b',') => self.i += 1,
                        Some(b'}') => {
                            self.i += 1;
                            return Ok(Field::Other);
                        }
                        _ => return Err(()),
                    }
                }
            }
            b'[' => {
                self.i += 1;
                self.ws();
                if self.peek() == Some(b']') {
                    self.i += 1;
                    return Ok(Field::Other);
                }
                loop {
                    self.value(depth + 1, false)?;
                    self.ws();
                    match self.peek() {
                        Some(b',') => self.i += 1,
                        Some(b']') => {
                            self.i += 1;
                            return Ok(Field::Other);
                        }
                        _ => return Err(()),
                    }
                }
            }
            b'"' => {
                if want {
                    let mut s = String::new();
                    let ok = self.string(Some(&mut s))?;
                    Ok(Field::Str(s, ok))
                } else {
                    self.string(None)?;
                    Ok(Field::Other)
                }
            }
            b't' => self.lit(b"true").map(|_| Field::Other),
            b'f' => self.lit(b"false").map(|_| Field::Other),
            b'n' => self.lit(b"null").map(|_| Field::Other),
            b'N' => self.lit(b"NaN").map(|_| Field::Other),
            b'I' => self.lit(b"Infinity").map(|_| Field::Other),
            b'-' if self.b.get(self.i + 1) == Some(&b'I') => self.lit(b"-Infinity").map(|_| Field::Other),
            b'-' | b'0'..=b'9' => Ok(match self.number()? {
                Some(n) => Field::Int(n),
                None => Field::Other,
            }),
            _ => Err(()),
        }
    }
}

/// Parses `body` as a JSON object and extracts the last occurrence of keys `k0`, `k1`.
/// Err(()) for invalid JSON or a non-object top level.
pub fn object2(body: &[u8], k0: &str, k1: &str) -> Result<(Field, Field), ()> {
    let body = body.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(body);
    std::str::from_utf8(body).map_err(|_| ())?;
    let mut p = P { b: body, i: 0 };
    let mut f0 = Field::Missing;
    let mut f1 = Field::Missing;
    p.ws();
    if p.peek() != Some(b'{') {
        // Validate anyway? Any non-object is a 400 regardless.
        return Err(());
    }
    p.i += 1;
    p.ws();
    if p.peek() == Some(b'}') {
        p.i += 1;
    } else {
        let mut key = String::new();
        loop {
            p.ws();
            if p.peek() != Some(b'"') {
                return Err(());
            }
            key.clear();
            let kok = p.string(Some(&mut key))?;
            p.ws();
            if p.peek() != Some(b':') {
                return Err(());
            }
            p.i += 1;
            if kok && key == k0 {
                f0 = p.value(1, true)?;
            } else if kok && key == k1 {
                f1 = p.value(1, true)?;
            } else {
                p.value(1, false)?;
            }
            p.ws();
            match p.peek() {
                Some(b',') => p.i += 1,
                Some(b'}') => {
                    p.i += 1;
                    break;
                }
                _ => return Err(()),
            }
        }
    }
    p.ws();
    if p.i != body.len() {
        return Err(());
    }
    Ok((f0, f1))
}

/// Appends `s` as a JSON string literal.
pub fn esc(out: &mut Vec<u8>, s: &[u8]) {
    out.push(b'"');
    let mut start = 0;
    for (i, &c) in s.iter().enumerate() {
        if c >= 0x20 && c != b'"' && c != b'\\' {
            continue;
        }
        out.extend_from_slice(&s[start..i]);
        match c {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            _ => {
                const H: &[u8; 16] = b"0123456789abcdef";
                out.extend_from_slice(b"\\u00");
                out.push(H[(c >> 4) as usize]);
                out.push(H[(c & 15) as usize]);
            }
        }
        start = i + 1;
    }
    out.extend_from_slice(&s[start..]);
    out.push(b'"');
}

#[inline]
pub fn num(out: &mut Vec<u8>, mut n: u64) {
    let mut buf = [0u8; 20];
    let mut i = 20;
    loop {
        i -= 1;
        buf[i] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    out.extend_from_slice(&buf[i..]);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(s: &str) -> bool {
        object2(s.as_bytes(), "a", "b").is_ok()
    }

    #[test]
    fn parses() {
        assert!(ok("{}"));
        assert!(ok(" {\"a\":1, \"x\":[1,2,{\"y\":null}], \"b\":\"q\"} "));
        assert!(!ok("{"));
        assert!(!ok(""));
        assert!(!ok("[]"));
        assert!(!ok("\"x\""));
        assert!(!ok("{\"a\":01}"));
        assert!(!ok("{\"a\":1,}"));
        assert!(!ok("{\"a\":\"\n\"}"));
        assert!(ok("{\"z\":NaN}"));
        match object2(br#"{"a":"\u0041\ud83e\udd86","b":9223372036854775807}"#, "a", "b").unwrap() {
            (Field::Str(s, true), Field::Int(n)) => {
                assert_eq!(s, "A\u{1F986}");
                assert_eq!(n, i64::MAX);
            }
            _ => panic!(),
        }
        match object2(br#"{"a":"\ud800","b":18446744073709551616}"#, "a", "b").unwrap() {
            (Field::Str(_, false), Field::Other) => {}
            _ => panic!(),
        }
        match object2(br#"{"a":1.0,"b":true,"a":2}"#, "a", "b").unwrap() {
            (Field::Int(2), Field::Other) => {}
            _ => panic!(),
        }
    }

    #[test]
    fn escapes() {
        let mut v = Vec::new();
        esc(&mut v, "a\"\\\n\u{1}é".as_bytes());
        assert_eq!(std::str::from_utf8(&v).unwrap(), "\"a\\\"\\\\\\n\\u0001é\"");
    }
}
