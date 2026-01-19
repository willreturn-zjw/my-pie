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
    let input_str: String = args.value_from_str("--input").unwrap_or_default();

    let input_data: AgentInput = if input_str.trim().is_empty() {
        eprintln!("[Generator] Warning: No input provided. Using Mock.");
        AgentInput {
            run_id: "mock_run".into(),
            node_id: "generator".into(),
            input_context: HashMap::from([("topic".to_string(), "The future of AI".to_string())]),
            upstream_results: HashMap::new(),
        }
    } else {
        serde_json::from_str(&input_str).map_err(|e| {
            eprintln!("[Generator] JSON Error: {}", e);
            e
        })?
    };
    
    // [Fix 1] 获取 Scheduler 注入的 Cache Header (包含 SAVE 指令)
    let mut header_tags = String::new();
    if let Some(tags) = input_data.input_context.get("_ctx_header") {
        header_tags = tags.clone();
    } else {
        // 回退逻辑：手动拼接 node_id 以确保唯一性 (防止并行 Generator 冲突)
        header_tags = format!("[SAVE:{}_{}]", input_data.run_id, input_data.node_id);
    }

    let topic = input_data.input_context.get("topic")
        .map(|s| s.as_str())
        .unwrap_or("AI");
    
    let model = inferlet::get_auto_model();
    let mut ctx = model.create_context();

    // [Fix 2] 将 Header 放在 System Prompt 最前面
    // 这样后端 server.py 才能正确解析 [SAVE:...] 指令
    let system_prompt = format!("{}You are a creative writer.", header_tags);
    
    ctx.fill_system(&system_prompt);
    ctx.fill_user(&format!("Write a short paragraph about {}.", topic));

    let sampler = Sampler::top_p(0.8, 0.95);
    let stop_cond = max_len(256).or(ends_with_any(model.eos_tokens()));

    let generated_text: String = ctx.generate(sampler, stop_cond).await;
    eprintln!("[Generator] Output: {}", generated_text);

    let output = AgentOutput {
        node_id: input_data.node_id.clone(),
        content: generated_text.clone(),
        status: "success".to_string(),
    };
    
    let kvs_key = format!("{}:{}", input_data.run_id, input_data.node_id);
    let kvs_value = serde_json::to_string(&output).unwrap();
    
    store_set(&kvs_key, &kvs_value);
    println!("Output written to KVS: {}", kvs_key);

    Ok(generated_text)
}