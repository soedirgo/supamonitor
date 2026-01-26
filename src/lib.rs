use heapless::index_map::FnvIndexMap;
use pgrx::{
    lwlock::PgLwLock,
    pg_shmem_init,
    pg_sys::{ffi::pg_guard_ffi_boundary, EnableQueryId},
    prelude::*,
    shmem::*,
};

pgrx::pg_module_magic!(name, version);

#[derive(Copy, Clone, Debug)]
pub struct SmQuery {
    query_text: [u8; 256],
    num_calls: usize,
}

impl Default for SmQuery {
    fn default() -> Self {
        Self {
            query_text: [0u8; 256],
            num_calls: 0,
        }
    }
}

unsafe impl PGRXSharedMemory for SmQuery {}

static SUPAMONITOR_SHARED_STATE: PgLwLock<AssertPGRXSharedMemory<FnvIndexMap<i32, SmQuery, 1024>>> =
    unsafe { PgLwLock::new(c"supamonitor") };

#[pg_guard]
pub unsafe extern "C-unwind" fn _PG_init() {
    if !pgrx::pg_sys::process_shared_preload_libraries_in_progress {
        pgrx::error!("this extension must be loaded via shared_preload_libraries.");
    }
    EnableQueryId();

    pg_shmem_init!(
        SUPAMONITOR_SHARED_STATE = unsafe { AssertPGRXSharedMemory::new(Default::default()) }
    );

    // post_parse_analyze_hook
    static mut PREV_POST_PARSE_ANALYZE_HOOK: pg_sys::post_parse_analyze_hook_type = None;
    PREV_POST_PARSE_ANALYZE_HOOK = pg_sys::post_parse_analyze_hook;
    pg_sys::post_parse_analyze_hook = Some(post_parse_analyze_hook);

    #[pg_guard]
    unsafe extern "C-unwind" fn post_parse_analyze_hook(
        parse_state: *mut pg_sys::ParseState,
        query: *mut pg_sys::Query,
        jumble_state: *mut pg_sys::JumbleState,
    ) {
        if let Some(prev_hook) = PREV_POST_PARSE_ANALYZE_HOOK {
            pg_guard_ffi_boundary(|| prev_hook(parse_state, query, jumble_state));
        }
    }

    // planner_hook
    static mut PREV_PLANNER_HOOK: pg_sys::planner_hook_type = None;
    PREV_PLANNER_HOOK = pg_sys::planner_hook;
    pg_sys::planner_hook = Some(planner_hook);

    #[pg_guard]
    unsafe extern "C-unwind" fn planner_hook(
        parse: *mut pg_sys::Query,
        query_string: *const std::ffi::c_char,
        cursor_options: std::ffi::c_int,
        bound_params: *mut pg_sys::ParamListInfoData,
    ) -> *mut pg_sys::PlannedStmt {
        if let Some(prev_hook) = PREV_PLANNER_HOOK {
            pg_guard_ffi_boundary(|| prev_hook(parse, query_string, cursor_options, bound_params))
        } else {
            pg_guard_ffi_boundary(|| {
                pg_sys::standard_planner(parse, query_string, cursor_options, bound_params)
            })
        }
    }

    // ExecutorStart_hook
    static mut PREV_EXECUTOR_START_HOOK: pg_sys::ExecutorStart_hook_type = None;
    PREV_EXECUTOR_START_HOOK = pg_sys::ExecutorStart_hook;
    pg_sys::ExecutorStart_hook = Some(executor_start_hook);

    #[pg_guard]
    unsafe extern "C-unwind" fn executor_start_hook(
        query_desc: *mut pg_sys::QueryDesc,
        eflags: std::ffi::c_int,
    ) {
        if let Some(prev_hook) = PREV_EXECUTOR_START_HOOK {
            pg_guard_ffi_boundary(|| prev_hook(query_desc, eflags));
        } else {
            pg_guard_ffi_boundary(|| pg_sys::standard_ExecutorStart(query_desc, eflags));
        }
    }

    // ExecutorRun_hook
    static mut PREV_EXECUTOR_RUN_HOOK: pg_sys::ExecutorRun_hook_type = None;
    PREV_EXECUTOR_RUN_HOOK = pg_sys::ExecutorRun_hook;
    pg_sys::ExecutorRun_hook = Some(executor_run_hook);

    #[pg_guard]
    unsafe extern "C-unwind" fn executor_run_hook(
        query_desc: *mut pg_sys::QueryDesc,
        direction: pg_sys::ScanDirection::Type,
        count: u64,
        execute_once: bool,
    ) {
        if let Some(prev_hook) = PREV_EXECUTOR_RUN_HOOK {
            pg_guard_ffi_boundary(|| prev_hook(query_desc, direction, count, execute_once));
        } else {
            pg_guard_ffi_boundary(|| {
                pg_sys::standard_ExecutorRun(query_desc, direction, count, execute_once)
            });
        }
    }

    // ExecutorFinish_hook
    static mut PREV_EXECUTOR_FINISH_HOOK: pg_sys::ExecutorFinish_hook_type = None;
    PREV_EXECUTOR_FINISH_HOOK = pg_sys::ExecutorFinish_hook;
    pg_sys::ExecutorFinish_hook = Some(executor_finish_hook);

    #[pg_guard]
    unsafe extern "C-unwind" fn executor_finish_hook(query_desc: *mut pg_sys::QueryDesc) {
        if let Some(prev_hook) = PREV_EXECUTOR_FINISH_HOOK {
            pg_guard_ffi_boundary(|| prev_hook(query_desc));
        } else {
            pg_guard_ffi_boundary(|| pg_sys::standard_ExecutorFinish(query_desc));
        }
    }

    // ExecutorEnd_hook
    static mut PREV_EXECUTOR_END_HOOK: pg_sys::ExecutorEnd_hook_type = None;
    PREV_EXECUTOR_END_HOOK = pg_sys::ExecutorEnd_hook;
    pg_sys::ExecutorEnd_hook = Some(executor_end_hook);

    #[pg_guard]
    unsafe extern "C-unwind" fn executor_end_hook(query_desc: *mut pg_sys::QueryDesc) {
        if let Some(prev_hook) = PREV_EXECUTOR_END_HOOK {
            pg_guard_ffi_boundary(|| prev_hook(query_desc));
        } else {
            pg_guard_ffi_boundary(|| pg_sys::standard_ExecutorEnd(query_desc));
        }
    }

    // ProcessUtility_hook
    static mut PREV_PROCESS_UTILITY_HOOK: pg_sys::ProcessUtility_hook_type = None;
    PREV_PROCESS_UTILITY_HOOK = pg_sys::ProcessUtility_hook;
    pg_sys::ProcessUtility_hook = Some(process_utility_hook);

    #[pg_guard]
    unsafe extern "C-unwind" fn process_utility_hook(
        pstmt: *mut pg_sys::PlannedStmt,
        query_string: *const std::ffi::c_char,
        read_only_tree: bool,
        context: pg_sys::ProcessUtilityContext::Type,
        params: *mut pg_sys::ParamListInfoData,
        query_env: *mut pg_sys::QueryEnvironment,
        dest: *mut pg_sys::DestReceiver,
        qc: *mut pg_sys::QueryCompletion,
    ) {
        if let Some(prev_hook) = PREV_PROCESS_UTILITY_HOOK {
            pg_guard_ffi_boundary(|| {
                prev_hook(
                    pstmt,
                    query_string,
                    read_only_tree,
                    context,
                    params,
                    query_env,
                    dest,
                    qc,
                )
            });
        } else {
            pg_guard_ffi_boundary(|| {
                pg_sys::standard_ProcessUtility(
                    pstmt,
                    query_string,
                    read_only_tree,
                    context,
                    params,
                    query_env,
                    dest,
                    qc,
                )
            });
        }
    }
}
