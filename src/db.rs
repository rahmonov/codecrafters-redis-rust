use std::path::PathBuf;

use anyhow::Result;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
};

pub async fn get_rdb_keys(rdb_file: PathBuf) -> Result<Vec<String>> {
    if !rdb_file.exists() {
        return Ok(Vec::new());
    }

    let file = File::open(rdb_file).await?;
    let mut reader = BufReader::new(file);
    let mut keys = Vec::new();
    while let Ok(section_type) = reader.read_u8().await {
        if section_type == 0xFF {
            break;
        }
        keys.extend(parse_database_section(&mut reader).await?);
    }

    Ok(keys)
}

pub async fn parse_database_section(reader: &mut BufReader<File>) -> Result<Vec<String>> {
    let mut buffer = Vec::with_capacity(4);
    let _ = reader.read_until(0xFB, &mut buffer).await?;
    let num_kvs = parse_size_encoding(reader).await?;
    let num_kvs_with_expiry = parse_size_encoding(reader).await?;

    let mut keybuf: Vec<String> = Vec::with_capacity(num_kvs);

    for _ in 0..num_kvs - num_kvs_with_expiry {
        let value_type = reader.read_u8().await?;
        let key = decode_string(reader).await?;
        let value = decode_string(reader).await?;
        keybuf.push(key);
    }

    for _ in 0..num_kvs_with_expiry {
        let expiry_type = reader.read_u8().await?;
        let expiry = match expiry_type {
            0xFC => {
                let expiry = reader.read_u64().await?;
                Some(expiry)
            }
            0xFD => {
                let expiry = reader.read_u32().await?;
                Some(expiry as u64)
            }
            _ => Err(anyhow::anyhow!("invalid expiry type"))?,
        };

        let value_type = reader.read_u8().await?;
        let key = decode_string(reader).await?;
        let value = decode_string(reader).await?;
        keybuf.push(key);
    }

    Ok(keybuf)
}

async fn decode_string(reader: &mut BufReader<File>) -> Result<String> {
    let length = parse_size_encoding(reader).await?;
    let mut buffer = vec![0; length];
    reader.read_exact(&mut buffer).await?;
    Ok(String::from_utf8(buffer)?)
}

async fn parse_size_encoding(reader: &mut BufReader<File>) -> Result<usize> {
    let mut buffer = Vec::with_capacity(4);
    let encode_pattern = reader.read_u8().await?;

    match encode_pattern >> 6 {
        0b00 => {
            buffer.push(encode_pattern & 0b0011_1111);
            Ok(buffer[0] as usize)
        }
        0b01 => {
            buffer.push(encode_pattern & 0b0011_1111);
            buffer.push(reader.read_u8().await?);
            Ok(u16::from_be_bytes([buffer[0], buffer[1]]) as usize)
        }
        0b10 => {
            for _ in 0..3 {
                buffer.push(reader.read_u8().await?);
            }
            Ok(u32::from_be_bytes([buffer[0], buffer[1], buffer[2], 0]) as usize)
        }
        _ => Err(anyhow::anyhow!("invalid encoding pattern")),
    }
}
