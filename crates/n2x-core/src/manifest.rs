
use std::collections::HashMap;


pub struct Manifest {
    pub from: usize,
    pub to: usize,
    pub mode: Mode,
    pub lifecycle: Lifecycle,
    /// 扩展元数据,不参与执行。可以用描述
    pub metadata: HashMap<String, String>,
}

pub struct Lifecycle {}

pub enum Mode {
    Schema(),
}
