// This is the interface to the JVM that we'll
// call the majority of our methods on.
use jni::JNIEnv;

// These objects are what you should use as arguments to your native function.
// They carry extra lifetime information to prevent them escaping this context
// and getting used after being GC'd.
use jni::objects::{GlobalRef, JClass, JObject, JString};

// This is just a pointer. We'll be returning it from our function.
// We can't return one of the objects with lifetime information because the
// lifetime checker won't let us.
use jni::sys::{jbyteArray, jint, jlong, jstring};

use std::{sync::mpsc, thread, time::Duration};

use datafusion::dataframe::DataFrame;
use datafusion::execution::context::{ExecutionConfig, ExecutionContext};
use std::sync::Arc;

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DefaultExecutionContext_querySql(
    env: JNIEnv,
    _class: JClass,
    callback: JObject,
    pointer: jlong,
    invocation_id: jlong,
    sql: JString,
) -> jlong {
    let sql: String = env
        .get_string(sql)
        .expect("Couldn't get sql as string!")
        .into();
    let context = unsafe { &mut *(pointer as *mut ExecutionContext) };
    let query_result = context.sql(&sql);
    match query_result {
        Ok(v) => Box::into_raw(Box::new(v)) as jlong,
        Err(err) => {
            let err_message = env
                .new_string(err.to_string())
                .expect("Couldn't create java string!");
            env.call_method(
                callback,
                "onErrorMessage",
                "(JLjava/lang/String;)V",
                &[invocation_id.into(), err_message.into()],
            )
            .unwrap();
            -1_i64
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_ExecutionContexts_destroyExecutionContext(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    let _ = unsafe { Box::from_raw(pointer as *mut ExecutionContext) };
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_ExecutionContexts_createExecutionContext(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let context = ExecutionContext::new();
    Box::into_raw(Box::new(context)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DataFrames_destroyDataFrame(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    let _ = unsafe { Box::from_raw(pointer as *mut Arc<dyn DataFrame>) };
}
