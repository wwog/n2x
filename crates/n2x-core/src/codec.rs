//! 模型 v1 的规范编码：递归按键排序的 JSON UTF-8，再计算 SHA-256。
//!
//! Encoded.bytes 被当作不透明数据；不会重写其中的 JSON 或改变业务编码。
use crate::*;
use serde::Serialize;
use sha2::{Digest as _, Sha256};

/// 生成规范 JSON 字节；只支持模型可序列化值，不接受浮点非有限值。
pub fn canonical<T: Serialize>(value: &T) -> Result<Vec<u8>, N2xError> {
    let mut value = serde_json::to_value(value).map_err(codec_error)?;
    // 也保证下游启用 serde_json/preserve_order 后摘要保持一致。
    value.sort_all_objects();
    serde_json::to_vec(&value).map_err(codec_error)
}

/// 对规范编码计算 SHA-256。
pub fn digest<T: Serialize>(value: &T) -> Result<Digest, N2xError> {
    Ok(Digest(Sha256::digest(canonical(value)?).into()))
}

/// 计算计划摘要，排除 digest 本身。
pub fn plan_digest(plan: &PlanSnapshot) -> Result<Digest, N2xError> {
    let mut value = serde_json::to_value(plan).map_err(codec_error)?;
    value
        .as_object_mut()
        .expect("plan is an object")
        .remove("digest");
    digest(&value)
}

/// 计算工作区事务摘要，排除 digest 本身。
pub fn commit_digest(commit: &WorkspaceCommit) -> Result<Digest, N2xError> {
    let mut value = serde_json::to_value(commit).map_err(codec_error)?;
    value
        .as_object_mut()
        .expect("commit is an object")
        .remove("digest");
    digest(&value)
}

fn codec_error(error: impl std::fmt::Display) -> N2xError {
    N2xError::new(
        "n2x.codec.invalid",
        ErrorKind::Integrity,
        ErrorOrigin::Engine,
        error.to_string(),
    )
}
