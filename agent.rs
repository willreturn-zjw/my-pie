use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// 调度器传给 Agent 的标准输入包
#[derive(Serialize, Deserialize, Debug)]
pub struct AgentInput {
    pub run_id: String,
    pub node_id: String,
    // 全局输入参数
    pub input_context: HashMap<String, String>,
    // 上游节点的输出结果 Map<NodeID, Content>
    pub upstream_results: HashMap<String, String>,
}

// Agent 的标准输出包（将被写入 KVS 或返回给调度器）
#[derive(Serialize, Deserialize, Debug)]
pub struct AgentOutput {
    pub node_id: String,
    pub content: String,
    pub status: String, // "success", "failed"
}