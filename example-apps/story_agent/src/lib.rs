use inferlet::{Args, Result};
use serde::{Deserialize, Serialize};
use inferlet::forward::{Forward};

// 1. 定义与 Python Scheduler 严格对应的输入结构
#[derive(Serialize, Deserialize, Debug)]
pub struct TaskInput {
    pub task_id: String,
    pub parent_task_id: Option<String>,
    
    // [修正] 对应 Python 中的 prompt 字段 (实际内容来自 JSON 的 instruction)
    pub prompt: String, 
    
    // [修正] 必须接收 mode 字段来决定行为
    // 如果 Python 没有传 mode，默认为 continue 以保持兼容
    #[serde(default = "default_mode")] 
    pub mode: String, 

    pub params: GenerationParams,
}

fn default_mode() -> String { "continue".to_string() }

#[derive(Serialize, Deserialize, Debug)]
pub struct GenerationParams {
    pub max_tokens: usize,
    pub temperature: f32,
    #[serde(default = "default_top_p")]
    pub top_p: f32,
}

fn default_top_p() -> f32 { 0.9 }

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TaskMetadata {
    pub token_ids: Vec<u32>,
}

#[inferlet::main]
async fn main(mut args: Args) -> Result<String> {
    // ==========================================
    // 1. 解析输入与协议校验
    // ==========================================
    let input_str: String = args.value_from_str("--input").unwrap_or_default();
    eprintln!("[Agent] Raw Input: {}", input_str);

    let req: TaskInput = serde_json::from_str(&input_str).map_err(|e| {
        eprintln!("[Agent] JSON Parse Error: {}", e);
        e
    })?;

    eprintln!("[Agent] Task: {} | Mode: {} | Prompt Len: {}", 
        req.task_id, req.mode, req.prompt.len());

    // ==========================================
    // 2. 初始化资源
    // ==========================================
    let model = inferlet::get_auto_model();
    let tokenizer = model.get_tokenizer();
    let queue = model.create_queue(); 

    // ==========================================
    // 3. 状态管理策略 (核心修正)
    // ==========================================
    // 根据 mode 决定是否继承历史
    let mut token_history = Vec::new();

    match req.mode.as_str() {
        "start" => {
            eprintln!("[Agent] Mode 'start': Ignoring history, starting fresh.");
            // 即使有 parent_task_id 也不加载，强制由 instruction 开头
        },
        "continue" | "merge" => {
            // continue: 正常的续写
            // merge: 在这个简化 Demo 中，我们假设 merge 也是接在某个分支后面进行总结
            if let Some(parent_id) = &req.parent_task_id {
                let meta_key = format!("{}_meta", parent_id);
                if let Some(meta_json) = inferlet::store_get(&meta_key) {
                    if let Ok(meta) = serde_json::from_str::<TaskMetadata>(&meta_json) {
                        eprintln!("[Agent] Mode '{}': Loaded history from parent {}: {} tokens.", 
                            req.mode, parent_id, meta.token_ids.len());
                        token_history = meta.token_ids;
                    } else {
                        eprintln!("[Agent] Warning: Failed to parse parent metadata.");
                    }
                } else {
                    eprintln!("[Agent] Warning: Parent metadata not found (key: {}). Starting fresh.", meta_key);
                }
            }
        },
        _ => {
            eprintln!("[Agent] Unknown mode '{}', treating as 'continue'", req.mode);
        }
    }

    // ==========================================
    // 4. KV Page 准备与重算 (Recompute Strategy)
    // ==========================================
    // 依然使用重算策略来规避 Copy/Export 的所有权复杂性
    let mut kv_pages = vec![queue.new_kv_page()];
    let page_size = model.get_kv_page_size() as usize;
    let mut last_page_len = 0;

    // 阶段一：Prefill (如果 History 不为空)
    if !token_history.is_empty() {
        let total_hist = token_history.len();
        let pages_needed = (total_hist + page_size - 1) / page_size;
        while kv_pages.len() < pages_needed {
            kv_pages.push(queue.new_kv_page());
        }
        
        let pass = queue.create_forward_pass();
        let positions: Vec<u32> = (0..total_hist).map(|i| i as u32).collect();
        pass.input_tokens(&token_history, &positions);
        pass.kv_cache(&kv_pages, 0);
        let _ = pass.execute().await;
        
        last_page_len = total_hist % page_size;
        eprintln!("[Agent] History restored (recomputed).");
    }

    // ==========================================
    // 5. 阶段二：生成 (Generation)
    // ==========================================
    
    // 构造当前 Prompt 的 Tokens
    // 注意：Workflow JSON 中的 "instruction" 会被 Scheduler 传入 req.prompt
    let input_tokens = tokenizer.tokenize(&req.prompt);
    
    // 如果是 Start 模式，输入就是整个故事的开头
    // 如果是 Continue 模式，输入是追加的指令或续写内容
    
    let mut generated_text = String::new();
    
    if !input_tokens.is_empty() {
        let mut current_pos = token_history.len() as u32;
        let mut tokens_to_process = input_tokens.clone();
        let max_gen = req.params.max_tokens;
        let mut gen_count = 0;
        let eos_token_sets = model.eos_tokens();

        while gen_count < max_gen {
            let pass = queue.create_forward_pass();
            let positions: Vec<u32> = (0..tokens_to_process.len())
                .map(|i| current_pos + i as u32)
                .collect();
            
            pass.input_tokens(&tokens_to_process, &positions);
            pass.kv_cache(&kv_pages, last_page_len);
            
            // 采样参数
            let last_idx = (tokens_to_process.len() - 1) as u32;
            pass.output_tokens_top_p(&[last_idx], req.params.temperature, req.params.top_p);

            let result = pass.execute().await;

            if let Some(out_tokens) = result.tokens {
                if let Some(&next_token) = out_tokens.first() {
                    token_history.extend_from_slice(&tokens_to_process);
                    current_pos += tokens_to_process.len() as u32;
                    
                    // 扩容检查
                    let current_total = token_history.len();
                    let pages_needed = (current_total + page_size - 1) / page_size;
                    while kv_pages.len() < pages_needed {
                        kv_pages.push(queue.new_kv_page());
                    }
                    last_page_len = current_total % page_size;

                    tokens_to_process = vec![next_token];
                    generated_text.push_str(&tokenizer.detokenize(&[next_token]));
                    gen_count += 1;

                    // EOS Check
                    let mut stopped = false;
                    for eos_seq in &eos_token_sets {
                        if eos_seq.contains(&next_token) {
                            stopped = true;
                            break;
                        }
                    }
                    if stopped { break; }
                } else { break; }
            } else { break; }
        }
        // 补全最后一步
        if !tokens_to_process.is_empty() {
            token_history.extend_from_slice(&tokens_to_process);
        }
    }

    // ==========================================
    // 6. Export 与清理
    // ==========================================
    eprintln!("[Agent] Exporting task_id: {}", req.task_id);
    queue.export_kv_pages(&kv_pages, &req.task_id);

    let meta = TaskMetadata { token_ids: token_history };
    let meta_json = serde_json::to_string(&meta).unwrap();
    inferlet::store_set(&format!("{}_meta", req.task_id), &meta_json);

    std::mem::forget(kv_pages);
    std::mem::forget(queue);

    Ok(generated_text)
}