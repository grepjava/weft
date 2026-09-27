//! Python types defined in Rust.
//!
//! `Ready` is an awaitable that is already complete. `await send(message)`
//! returns one: the bytes have gone into the write buffer (usually onto the
//! socket), so the coroutine resumes without a trip through the event loop.
//! When it carries no value it finishes by returning NULL with no exception
//! set, which CPython reads as `None`, so no `StopIteration` is created.
//!
//! `send`, `receive` and the task done-callback are C-level callables using
//! the vectorcall protocol, so `await send(message)` allocates no argument
//! tuple. They carry a token rather than a pointer: the connection's slot and
//! generation and the request's sequence number on that connection. They are
//! resolved through the worker that owns the calling thread, so an object an
//! application keeps after its connection has gone finds an empty slot rather
//! than someone else's request, and deallocating one on another thread (as a
//! free-threaded collector may) touches no server state.

use std::ffi::{CStr, c_int, c_void};
use std::ptr;
use std::sync::OnceLock;

use pyo3_ffi::*;

use crate::py::{PResult, PyErr, PyRef, Static};

#[repr(C)]
pub struct ReadyObj {
    ob_base: PyObject,
    value: *mut PyObject,
    /// Yield to the event loop once before completing, as `asyncio.sleep(0)`
    /// does: a task that only ever awaits completed sends would otherwise
    /// never let the loop run anything else.
    yield_first: bool,
}

#[repr(C)]
pub struct ChanObj {
    ob_base: PyObject,
    vectorcall: Option<vectorcallfunc>,
    pub slot: u32,
    pub generation: u32,
    pub seq: u32,
}

pub struct Types {
    pub ready: Static,
    pub send: Static,
    pub receive: Static,
    pub done: Static,
    /// `weft.ClientDisconnected(OSError)`: sending on a closed WebSocket.
    pub client_disconnected: Static,
}

static TYPES: OnceLock<Types> = OnceLock::new();

#[inline]
pub fn types() -> &'static Types {
    unsafe { TYPES.get().unwrap_unchecked() }
}

/// Creates a heap type from a spec whose storage is leaked: it lives as long
/// as the process, which is how long the type does.
pub unsafe fn new_type(
    name: &'static CStr,
    basicsize: usize,
    flags: std::ffi::c_ulong,
    mut slots: Vec<PyType_Slot>,
) -> PResult<*mut PyObject> {
    slots.push(PyType_Slot {
        slot: 0,
        pfunc: ptr::null_mut(),
    });
    let slots: &'static mut [PyType_Slot] = Box::leak(slots.into_boxed_slice());
    let spec: &'static mut PyType_Spec = Box::leak(Box::new(PyType_Spec {
        name: name.as_ptr(),
        basicsize: basicsize as c_int,
        itemsize: 0,
        flags: flags as std::ffi::c_uint,
        slots: slots.as_mut_ptr(),
    }));
    let t = unsafe { PyType_FromSpec(spec) };
    if t.is_null() { Err(PyErr) } else { Ok(t) }
}

#[inline]
pub fn slot(slot: c_int, f: *mut c_void) -> PyType_Slot {
    PyType_Slot { slot, pfunc: f }
}

/// Allocates an instance of a type with no GC tracking and no `__dict__`.
#[inline]
pub unsafe fn alloc(tp: *mut PyObject, size: usize) -> PResult<*mut PyObject> {
    unsafe {
        let p = PyObject_Malloc(size) as *mut PyObject;
        if p.is_null() {
            PyErr_NoMemory();
            return Err(PyErr);
        }
        ptr::write_bytes(p as *mut u8, 0, size);
        Ok(PyObject_Init(p, tp as *mut PyTypeObject))
    }
}

unsafe extern "C" fn plain_dealloc(o: *mut PyObject) {
    unsafe {
        let tp = Py_TYPE(o);
        PyObject_Free(o as *mut c_void);
        Py_DECREF(tp as *mut PyObject);
    }
}

// --- Ready -------------------------------------------------------------------

unsafe extern "C" fn ready_dealloc(o: *mut PyObject) {
    unsafe {
        let r = o as *mut ReadyObj;
        let v = (*r).value;
        (*r).value = ptr::null_mut();
        Py_XDECREF(v);
        plain_dealloc(o);
    }
}

unsafe extern "C" fn ready_await(o: *mut PyObject) -> *mut PyObject {
    unsafe { Py_INCREF(o) };
    o
}

/// Takes the value out; `NULL` and no exception means "returned None".
unsafe extern "C" fn ready_next(o: *mut PyObject) -> *mut PyObject {
    unsafe {
        let r = o as *mut ReadyObj;
        if (*r).yield_first {
            // A bare `yield`: the task is rescheduled with `call_soon`.
            (*r).yield_first = false;
            return crate::py::none().into_ptr();
        }
        let v = (*r).value;
        if !v.is_null() {
            (*r).value = ptr::null_mut();
            // StopIteration(value): a dict is never mistaken for an args tuple
            // or an exception instance, which PyErr_SetObject would unpack.
            let exc = PyObject_CallOneArg(PyExc_StopIteration, v);
            Py_DECREF(v);
            if !exc.is_null() {
                PyErr_SetObject(PyExc_StopIteration, exc);
                Py_DECREF(exc);
            }
        }
        ptr::null_mut()
    }
}

/// `am_send`, used by `await` on 3.10 and 3.11: hands the value back with
/// no exception at all.
unsafe extern "C" fn ready_send(
    o: *mut PyObject,
    _arg: *mut PyObject,
    result: *mut *mut PyObject,
) -> PySendResult {
    unsafe {
        let r = o as *mut ReadyObj;
        if (*r).yield_first {
            (*r).yield_first = false;
            *result = crate::py::none().into_ptr();
            return PySendResult::PYGEN_NEXT;
        }
        let v = (*r).value;
        if v.is_null() {
            Py_INCREF(Py_None());
            *result = Py_None();
        } else {
            (*r).value = ptr::null_mut();
            *result = v;
        }
        PySendResult::PYGEN_RETURN
    }
}

unsafe extern "C" fn ready_send_method(o: *mut PyObject, _arg: *mut PyObject) -> *mut PyObject {
    unsafe { ready_next(o) }
}

unsafe extern "C" fn ready_throw(
    _o: *mut PyObject,
    args: *mut *mut PyObject,
    nargs: Py_ssize_t,
) -> *mut PyObject {
    unsafe {
        if nargs < 1 {
            PyErr_SetString(
                PyExc_TypeError,
                c"throw expected at least 1 argument".as_ptr(),
            );
            return ptr::null_mut();
        }
        let typ = *args;
        if PyExceptionInstance_Check(typ) != 0 {
            PyErr_SetObject(PyExceptionInstance_Class(typ), typ);
        } else if nargs >= 2 && !crate::py::is_none(*args.add(1)) {
            PyErr_SetObject(typ, *args.add(1));
        } else {
            PyErr_SetNone(typ);
        }
        ptr::null_mut()
    }
}

unsafe extern "C" fn ready_close(_o: *mut PyObject, _a: *mut PyObject) -> *mut PyObject {
    unsafe { crate::py::none().into_ptr() }
}

/// A complete awaitable returning `value` (stolen), or `None` when null.
pub unsafe fn ready(value: Option<PyRef>) -> PResult<PyRef> {
    unsafe {
        let o = alloc(types().ready.0, std::mem::size_of::<ReadyObj>())?;
        (*(o as *mut ReadyObj)).value = value.map_or(ptr::null_mut(), PyRef::into_ptr);
        PyRef::own(o)
    }
}

/// Like `ready(None)`, but yields to the event loop once first.
pub unsafe fn yield_once() -> PResult<PyRef> {
    unsafe {
        let r = ready(None)?;
        (*(r.ptr() as *mut ReadyObj)).yield_first = true;
        Ok(r)
    }
}

// --- send / receive / done ---------------------------------------------------

pub unsafe fn chan(
    kind: &Static,
    f: vectorcallfunc,
    slot: u32,
    generation: u32,
    seq: u32,
) -> PResult<PyRef> {
    unsafe {
        let o = alloc(kind.0, std::mem::size_of::<ChanObj>())?;
        let c = o as *mut ChanObj;
        (*c).vectorcall = Some(f);
        (*c).slot = slot;
        (*c).generation = generation;
        (*c).seq = seq;
        PyRef::own(o)
    }
}

unsafe fn chan_type(name: &'static CStr) -> PResult<*mut PyObject> {
    let members: &'static mut [PyMemberDef] = Box::leak(Box::new([
        PyMemberDef {
            name: c"__vectorcalloffset__".as_ptr(),
            type_code: Py_T_PYSSIZET,
            offset: std::mem::offset_of!(ChanObj, vectorcall) as Py_ssize_t,
            flags: Py_READONLY,
            doc: ptr::null(),
        },
        unsafe { std::mem::zeroed() },
    ]));
    unsafe {
        new_type(
            name,
            std::mem::size_of::<ChanObj>(),
            Py_TPFLAGS_DEFAULT
                | Py_TPFLAGS_HAVE_VECTORCALL
                | Py_TPFLAGS_IMMUTABLETYPE
                | Py_TPFLAGS_DISALLOW_INSTANTIATION,
            vec![
                slot(Py_tp_dealloc, plain_dealloc as *mut c_void),
                slot(Py_tp_call, PyVectorcall_Call as *mut c_void),
                slot(Py_tp_members, members.as_mut_ptr() as *mut c_void),
            ],
        )
    }
}

pub unsafe fn init() -> PResult<()> {
    if TYPES.get().is_some() {
        return Ok(());
    }
    let methods: &'static mut [PyMethodDef] = Box::leak(Box::new([
        PyMethodDef {
            ml_name: c"send".as_ptr(),
            ml_meth: PyMethodDefPointer {
                PyCFunction: ready_send_method,
            },
            ml_flags: METH_O,
            ml_doc: ptr::null(),
        },
        PyMethodDef {
            ml_name: c"throw".as_ptr(),
            ml_meth: PyMethodDefPointer {
                PyCFunctionFast: ready_throw,
            },
            ml_flags: METH_FASTCALL,
            ml_doc: ptr::null(),
        },
        PyMethodDef {
            ml_name: c"close".as_ptr(),
            ml_meth: PyMethodDefPointer {
                PyCFunction: ready_close,
            },
            ml_flags: METH_NOARGS,
            ml_doc: ptr::null(),
        },
        PyMethodDef::zeroed(),
    ]));
    unsafe {
        let ready = new_type(
            c"weft._weft.Ready",
            std::mem::size_of::<ReadyObj>(),
            Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE | Py_TPFLAGS_DISALLOW_INSTANTIATION,
            vec![
                slot(Py_tp_dealloc, ready_dealloc as *mut c_void),
                slot(Py_am_await, ready_await as *mut c_void),
                slot(Py_am_send, ready_send as *mut c_void),
                slot(Py_tp_iter, ready_await as *mut c_void),
                slot(Py_tp_iternext, ready_next as *mut c_void),
                slot(Py_tp_methods, methods.as_mut_ptr() as *mut c_void),
            ],
        )?;
        let _ = TYPES.set(Types {
            ready: Static(ready),
            send: Static(chan_type(c"weft._weft.ASGISend")?),
            receive: Static(chan_type(c"weft._weft.ASGIReceive")?),
            done: Static(chan_type(c"weft._weft.TaskDone")?),
            client_disconnected: Static({
                let e = PyErr_NewException(
                    c"weft.ClientDisconnected".as_ptr(),
                    PyExc_OSError,
                    ptr::null_mut(),
                );
                if e.is_null() {
                    return Err(PyErr);
                }
                e
            }),
        });
    }
    Ok(())
}
