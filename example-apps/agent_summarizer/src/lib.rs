use inferlet::{Args, Result, Sampler, store_set};
// [Fix 2] 引入 StopCondition trait
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
    // [Fix 1] 改为 "--input"
    let input_str: String = args.value_from_str("--input").unwrap_or_default();

    let input_data: AgentInput = serde_json::from_str(&input_str).map_err(|e| {
        eprintln!("[Summarizer] JSON Error: {}", input_str);
        e
    })?;

    eprintln!("[Summarizer] Started...");

    let source_text = input_data.upstream_results.get("node_generator")
        .ok_or_else(|| anyhow::anyhow!("Missing upstream data from node_generator"))?;

    let model = inferlet::get_auto_model();
    let mut ctx = model.create_context();

    ctx.fill_system("You are a concise editor.");
    ctx.fill_user(&format!("Summarize this:\n\n\"{}\"", source_text));

    let sampler = Sampler::top_p(0.6, 0.95);
    let stop_cond = max_len(128).or(ends_with_any(model.eos_tokens()));

    // [Fix 3] 显式标注 : String
    let summary: String = ctx.generate(sampler, stop_cond).await;
    eprintln!("[Summarizer] Result: {}", summary);

    let output = AgentOutput {
        node_id: input_data.node_id.clone(),
        content: summary.clone(),
        status: "success".to_string(),
    };
    
    let kvs_key = format!("{}:{}", input_data.run_id, input_data.node_id);
    let kvs_value = serde_json::to_string(&output).unwrap();
    
    // [Fix 4] 移除 .as_bytes() 和 ?
    store_set(&kvs_key, &kvs_value);

    Ok(summary)
}