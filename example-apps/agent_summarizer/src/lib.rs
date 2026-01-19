use inferlet::{Args, Result, Sampler, store_set};
use inferlet::stop_condition::{ends_with_any, max_len, StopCondition};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug)]
pub struct AgentInput {
    pub run_id: String,
    pub node_id: String,
    pub input_context: HashMap<String, String>,
    pub upstream_results: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AgentOutput {
    pub node_id: String,
    pub content: String,
    pub status: String,
}

#[inferlet::main]
async fn main(mut args: Args) -> Result<String> {
    // 1. 获取输入参数
    let input_str: String = args.value_from_str("--input").unwrap_or_default();

    let input_data: AgentInput = serde_json::from_str(&input_str).map_err(|e| {
        eprintln!("[Summarizer] JSON Error: {}", input_str);
        e
    })?;

    eprintln!("[Summarizer] Started for node: {}", input_data.node_id);

    // =========================================================
    // [Fix 1: 动态处理上游数据]
    // 不再硬编码 get("node_generator")，而是遍历所有上游结果
    // =========================================================
    if input_data.upstream_results.is_empty() {
        return Err(anyhow::anyhow!("No upstream data received! I need at least one source.").into());
    }

    let mut combined_text = String::new();
    for (source_node, content) in &input_data.upstream_results {
        // 拼接格式，清晰区分不同来源
        combined_text.push_str(&format!("\n\n--- Source: {} ---\n{}", source_node, content));
    }

    // =========================================================
    // [Fix 2: 获取 Cache 控制头]
    // Scheduler 把 [LOAD:...][SAVE:...] 注入到了 input_context 中
    // 我们需要优先提取它，放在 System Prompt 最前面
    // =========================================================
    let mut header_tags = String::new();
    
    // 优先查找 Scheduler 注入的专用字段
    if let Some(tags) = input_data.input_context.get("_ctx_header") {
        header_tags = tags.clone();
    } else {
        // 兼容性回退：如果没有高级 tag，使用旧的 CID 格式
        header_tags = format!("[CID:{}]", input_data.run_id);
    }
    
    // 获取用户配置的风格，默认为 "Concise"
    let style = input_data.input_context.get("style").map(|s| s.as_str()).unwrap_or("Concise");

    // 3. 构建 Prompt
    let model = inferlet::get_auto_model();
    let mut ctx = model.create_context();

    // 【关键】header_tags 必须在最前面！
    let system_prompt = format!("{}You are a professional editor. Style requirement: {}.", header_tags, style);
    
    ctx.fill_system(&system_prompt);
    ctx.fill_user(&format!("Please summarize the following contents:\n{}", combined_text));

    // 4. 执行推理
    let sampler = Sampler::top_p(0.6, 0.95);
    let stop_cond = max_len(256).or(ends_with_any(model.eos_tokens()));

    let summary: String = ctx.generate(sampler, stop_cond).await;
    eprintln!("[Summarizer] Result: {}", summary);

    // 5. 保存结果
    let output = AgentOutput {
        node_id: input_data.node_id.clone(),
        content: summary.clone(),
        status: "success".to_string(),
    };
    
    let kvs_key = format!("{}:{}", input_data.run_id, input_data.node_id);
    let kvs_value = serde_json::to_string(&output).unwrap();
    
    store_set(&kvs_key, &kvs_value);

    Ok(summary)
}