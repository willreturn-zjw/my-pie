use inferlet::{
    forward::{Forward, KvPage},
    sampler::Sampler,
    stop_condition::{max_len, ends_with_any, StopCondition},
    Args, Queue, Result, Tokenizer, main, get_auto_model, store_set, Resource
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct AgentInput {
    task_id: String,
    #[serde(default)] 
    parent_task_ids: Vec<String>,
    prompt: String,
}

#[derive(Debug, Serialize)]
struct AgentMeta {
    token_ids: Vec<u32>,
    kv_page_last_len: usize,
    // 【新增】KV 依赖链。作为起始节点，这是链条的第一环。
    kv_chain: Vec<String>,
}

#[inferlet::main]
async fn main(mut args: Args) -> Result<String> {
    eprintln!("[Debug] Intro Agent (Chain Root) started.");
    
    // 1. 解析输入
    let input_json: String = args.value_from_str(["-i", "--input"])?;
    let input: AgentInput = serde_json::from_str(&input_json)
        .map_err(|e| anyhow::anyhow!("Failed to parse input JSON: {}", e))?;

    // 2. 初始化
    let model = get_auto_model();
    let mut ctx = model.create_context();

    // 3. 注入 Prompt
    ctx.fill_system("You are a fantasy novel writer.");
    ctx.fill_user(&input.prompt);

    // 4. 推理
    let sampler = Sampler::top_k_top_p(0.6, 20, 0.95);
    let stop_cond = max_len(1024).or(ends_with_any(model.eos_tokens()));

    let generated_text = ctx.generate(sampler, stop_cond).await;
    eprintln!("[Debug] Intro generation complete.");

    // 5. 状态保存
    let kv_resource_name = format!("{}_kv", input.task_id);
    
    // 导出显存 (Intro 产生的是链条的基础部分)
    ctx.queue().export_kv_pages(&ctx.kv_pages, &kv_resource_name);

    // 【新增】构建 KV 链
    // 因为是起始节点，链条里只有我自己产生的这一份 KV
    let my_chain = vec![kv_resource_name.clone()];

    // 导出元数据
    let meta = AgentMeta {
        token_ids: ctx.get_token_ids().to_vec(),
        kv_page_last_len: ctx.get_kv_page_last_len(),
        kv_chain: my_chain, // 存入链条
    };
    
    let meta_json = serde_json::to_string(&meta)?;
    store_set(&format!("{}_meta", input.task_id), &meta_json);
    store_set(&format!("{}_output", input.task_id), &generated_text);

    eprintln!("[Debug] Intro state saved with Chain initialized. Leaking context...");

    // 6. 遗忘 ctx，防止显存被释放
    std::mem::forget(ctx);

    Ok(generated_text)
}