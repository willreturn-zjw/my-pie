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

    // [Fix]: 使用 map(as_str) 避免临时变量生命周期问题
    let topic = input_data.input_context.get("topic")
        .map(|s| s.as_str())
        .unwrap_or("AI");
    
    let model = inferlet::get_auto_model();
    let mut ctx = model.create_context();

    ctx.fill_system("You are a creative writer.");
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