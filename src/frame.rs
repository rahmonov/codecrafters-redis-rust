use anyhow::Result;
use bytes::BytesMut;

#[derive(Clone, Debug)]
pub enum Frame {
    SimpleString(String),
    BulkString(String),
    Array(Vec<Frame>),
    NullBulkString,
}

impl Frame {
    pub fn serialize(self) -> String {
        match self {
            Frame::SimpleString(s) => format!("+{}\r\n", s),
            Frame::BulkString(s) => format!("${}\r\n{}\r\n", s.chars().count(), s),
            Frame::NullBulkString => "$-1\r\n".to_string(),
            Frame::Array(values) => format!(
                "*{}\r\n{}",
                values.len(),
                values
                    .into_iter()
                    .map(|val| val.serialize())
                    .collect::<Vec<String>>()
                    .join("")
            ),
        }
    }

    pub fn parse_message(buffer: BytesMut) -> Result<(Frame, usize)> {
        match buffer[0] as char {
            '+' => parse_simple_string(buffer),
            '*' => parse_array(buffer),
            '$' => parse_bulk_string(buffer),
            _ => Err(anyhow::anyhow!("Not a known value type {:?}", buffer)),
        }
    }
}

fn parse_simple_string(buffer: BytesMut) -> Result<(Frame, usize)> {
    if let Some((line, len)) = read_until_crlf(&buffer[1..]) {
        let string = String::from_utf8(line.to_vec()).unwrap();

        return Ok((Frame::SimpleString(string), len + 1));
    }

    Err(anyhow::anyhow!("Invalid string {:?}", buffer))
}

fn parse_array(buffer: BytesMut) -> Result<(Frame, usize)> {
    let (array_length, mut bytes_consumed) =
        if let Some((line, len)) = read_until_crlf(&buffer[1..]) {
            let array_length = parse_int(line)?;

            (array_length, len + 1)
        } else {
            return Err(anyhow::anyhow!("Invalid array format {:?}", buffer));
        };

    let mut items = Vec::with_capacity(array_length as usize);

    for _ in 0..array_length {
        let (array_item, len) = Frame::parse_message(BytesMut::from(&buffer[bytes_consumed..]))?;
        items.push(array_item);
        bytes_consumed += len;
    }

    Ok((Frame::Array(items), bytes_consumed))
}

fn parse_bulk_string(buffer: BytesMut) -> Result<(Frame, usize)> {
    let (bulk_str_len, bytes_consumed) = if let Some((line, len)) = read_until_crlf(&buffer[1..]) {
        let bulk_str_len = parse_int(line)?;

        (bulk_str_len, len + 1)
    } else {
        return Err(anyhow::anyhow!("Invalid array format: {:?}", buffer));
    };

    let end_of_bulk_str = bytes_consumed + bulk_str_len as usize;
    let total_parsed = end_of_bulk_str + 2;

    Ok((
        Frame::BulkString(String::from_utf8(
            buffer[bytes_consumed..end_of_bulk_str].to_vec(),
        )?),
        total_parsed,
    ))
}

fn read_until_crlf(buffer: &[u8]) -> Option<(&[u8], usize)> {
    for i in 1..buffer.len() {
        if buffer[i - 1] == b'\r' && buffer[i] == b'\n' {
            return Some((&buffer[0..(i - 1)], i + 1));
        }
    }

    None
}

fn parse_int(buffer: &[u8]) -> Result<i64> {
    Ok(String::from_utf8(buffer.to_vec())?.parse::<i64>()?)
}
