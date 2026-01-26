use inferlet::{Args, Result, Sampler, store_set};
use inferlet::stop_condition::{ends_with_any, max_len, StopCondition};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug)]
pub struct AgentInput {
    pub run_id: String,
    pub node_id: String,
    // [Fix] 接收 Scheduler 传来的 parent_id
    pub parent_node_id: Option<String>, 
    pub parent_node_instruction: Option<String>, 
    pub input_context: HashMap<String, String>,
    pub upstream_results: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AgentOutput {
    pub node_id: String,
    pub content: String,
}

#[inferlet::main]
async fn main(mut args: Args) -> Result<String> {
    let input_str: String = args.value_from_str("--input").unwrap_or_default();
    let input_data: AgentInput = serde_json::from_str(&input_str).map_err(|e| {
        eprintln!("[StoryAgent] JSON Error: {}", e);
        e
    })?;

    // === [CORE LOGIC] 构造 KV Cache 控制标签 ===
    let self_cid = format!("{}_{}", input_data.run_id, input_data.node_id);
    let save_tag = format!("[SAVE:{}]", self_cid);
    
    let load_tag = if let Some(parent) = &input_data.parent_node_id {
        let parent_cid = format!("{}_{}", input_data.run_id, parent);
        format!("[LOAD:{}]", parent_cid)
    } else {
        String::new()
    };

    // 将标签放在 System Prompt 的最最最前面！
    // 这样 Backend 解码前几个 token 时一定能看到。
    let base_system = "You are a fantasy novel writer.";
    let system_prompt = format!("{}{}{}", load_tag, save_tag, base_system);
    
    // ===========================================

    let model = inferlet::get_auto_model();
    let mut ctx = model.create_context();
    
    ctx.fill_system(&system_prompt);

    let mode = input_data.input_context.get("mode").map(|s| s.as_str()).unwrap_or("start");
    let instruction = input_data.input_context.get("instruction").unwrap_or(&"Write something.".to_string()).clone();

    match mode {
        "start" => {
            ctx.fill_user(&instruction);
        }
        "continue" => {
            // 复用逻辑：必须完全重现父节点的历史对话结构
            if let Some((_, parent_output)) = input_data.upstream_results.iter().next() {
                // 1. 父节点的输入 (为了演示，这里硬编码，实际应该从 upstream 传)
                ctx.fill_user(input_data.parent_node_instruction.as_deref().unwrap_or(""));
                // 2. 父节点的输出 (Prefill)
                ctx.fill_assistant(parent_output);
                // 3. 当前节点的指令
                ctx.fill_user(&instruction);
            } else {
                ctx.fill_user(&instruction);
            }
        }
        "merge" => {
            let mut combined = String::new();
            for (k, v) in &input_data.upstream_results {
                combined.push_str(&format!("Option [{}]: {}\n", k, v));
            }
            ctx.fill_user(&format!("Summarize these two endings:\n{}", combined));
        }
        _ => { ctx.fill_user(&instruction); }
    }

    let sampler = Sampler::top_p(0.0, 1.0);
    //let sampler = Sampler::top_p(0.6, 0.9);
    let stop_cond = max_len(128).or(ends_with_any(model.eos_tokens()));
    //let stop_cond = max_len(1024).or(ends_with_any(model.eos_tokens()));

    // 生成
    let generated: String = ctx.generate(sampler, stop_cond).await;
    
    // 清洗输出中的标签（以防万一模型把它输出了）
    let clean_generated = generated
        .replace(&save_tag, "")
        .replace(&load_tag, "")
        .replace("<|start_header_id|>", "")
        .trim()
        .to_string();

    eprintln!("[{}] Output len: {}", input_data.node_id, clean_generated.len());

    let output = AgentOutput {
        node_id: input_data.node_id.clone(),
        content: clean_generated.clone(),
    };
    let kvs_key = format!("{}:{}", input_data.run_id, input_data.node_id);
    store_set(&kvs_key, &serde_json::to_string(&output).unwrap());

    Ok(clean_generated)
}