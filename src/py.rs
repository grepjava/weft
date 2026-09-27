//! The CPython layer: owned references, calls, constructors and errors.
//!
//! Python objects are never wrapped in anything reference counted on the Rust
//! side. A `PyRef` owns exactly one strong reference and releases it in
//! `Drop`; a borrowed reference is a bare `*mut PyObject`. Every function here
//! requires the calling thread to be attached to the interpreter (hold the GIL).

use std::ffi::{CStr, CString, c_char};
use std::ptr::{self, NonNull};

use pyo3_ffi::*;

/// A Python exception is set on the current thread.
#[derive(Debug)]
pub struct PyErr;

pub type PResult<T> = Result<T, PyErr>;

#[repr(transparent)]
pub struct PyRef(NonNull<PyObject>);

impl PyRef {
    /// Takes ownership of a new reference; `NULL` means an exception is set.
    #[inline]
    pub unsafe fn own(p: *mut PyObject) -> PResult<Self> {
        NonNull::new(p).map(PyRef).ok_or(PyErr)
    }

    #[inline]
    pub unsafe fn borrow(p: *mut PyObject) -> Self {
        unsafe { Py_INCREF(p) };
        PyRef(unsafe { NonNull::new_unchecked(p) })
    }

    #[inline]
    pub fn ptr(&self) -> *mut PyObject {
        self.0.as_ptr()
    }

    #[inline]
    pub fn into_ptr(self) -> *mut PyObject {
        let p = self.0.as_ptr();
        std::mem::forget(self);
        p
    }
}

impl Clone for PyRef {
    #[inline]
    fn clone(&self) -> Self {
        unsafe { Self::borrow(self.ptr()) }
    }
}

impl Drop for PyRef {
    #[inline]
    fn drop(&mut self) {
        unsafe { Py_DECREF(self.0.as_ptr()) }
    }
}

/// A raw object pointer created once per process and never released, so it
/// may sit in a static and be read from any worker thread.
#[derive(Clone, Copy)]
pub struct Static(pub *mut PyObject);
unsafe impl Send for Static {}
unsafe impl Sync for Static {}

#[inline]
pub unsafe fn none() -> PyRef {
    unsafe { PyRef::borrow(Py_None()) }
}

#[inline]
pub unsafe fn is_none(o: *mut PyObject) -> bool {
    o == unsafe { Py_None() }
}

#[inline]
pub unsafe fn bytes(b: &[u8]) -> PResult<PyRef> {
    unsafe {
        PyRef::own(PyBytes_FromStringAndSize(
            b.as_ptr().cast(),
            b.len() as Py_ssize_t,
        ))
    }
}

/// A `str` from bytes the caller knows to be ASCII.
#[inline]
pub unsafe fn str_ascii(b: &[u8]) -> PResult<PyRef> {
    unsafe {
        PyRef::own(PyUnicode_FromStringAndSize(
            b.as_ptr().cast(),
            b.len() as Py_ssize_t,
        ))
    }
}

#[inline]
pub unsafe fn str_utf8_replace(b: &[u8]) -> PResult<PyRef> {
    unsafe {
        PyRef::own(PyUnicode_DecodeUTF8(
            b.as_ptr().cast(),
            b.len() as Py_ssize_t,
            c"replace".as_ptr(),
        ))
    }
}

#[inline]
pub unsafe fn int(v: i64) -> PResult<PyRef> {
    unsafe { PyRef::own(PyLong_FromLongLong(v)) }
}

pub unsafe fn intern(s: &CStr) -> PResult<PyRef> {
    unsafe { PyRef::own(PyUnicode_InternFromString(s.as_ptr())) }
}

#[inline]
pub unsafe fn tuple2(a: PyRef, b: PyRef) -> PResult<PyRef> {
    unsafe {
        let t = PyRef::own(PyTuple_New(2))?;
        PyTuple_SET_ITEM(t.ptr(), 0, a.into_ptr());
        PyTuple_SET_ITEM(t.ptr(), 1, b.into_ptr());
        Ok(t)
    }
}

/// `d[k] = v`, consuming `v`.
#[inline]
pub unsafe fn dict_set(d: *mut PyObject, k: *mut PyObject, v: PyRef) -> PResult<()> {
    if unsafe { PyDict_SetItem(d, k, v.ptr()) } < 0 {
        Err(PyErr)
    } else {
        Ok(())
    }
}

/// Borrowed `d[k]`, `None` when absent. Works on any mapping; dicts take
/// the fast path, which is every ASGI message a framework sends.
pub unsafe fn dict_get(d: *mut PyObject, k: *mut PyObject) -> PResult<Option<Borrowed>> {
    unsafe {
        if PyDict_Check(d) != 0 {
            let v = PyDict_GetItemWithError(d, k);
            if v.is_null() {
                return if PyErr_Occurred().is_null() {
                    Ok(None)
                } else {
                    Err(PyErr)
                };
            }
            return Ok(Some(Borrowed::Dict(v)));
        }
        let v = PyObject_GetItem(d, k);
        if v.is_null() {
            if PyErr_ExceptionMatches(PyExc_KeyError) != 0 {
                PyErr_Clear();
                return Ok(None);
            }
            return Err(PyErr);
        }
        Ok(Some(Borrowed::Owned(PyRef::own(v)?)))
    }
}

/// A value looked up in a mapping: borrowed from a dict, or owned when the
/// mapping had to go through `__getitem__`.
pub enum Borrowed {
    Dict(*mut PyObject),
    Owned(PyRef),
}

impl Borrowed {
    #[inline]
    pub fn ptr(&self) -> *mut PyObject {
        match self {
            Borrowed::Dict(p) => *p,
            Borrowed::Owned(r) => r.ptr(),
        }
    }
}

#[inline]
pub unsafe fn call(f: *mut PyObject, args: &[*mut PyObject]) -> PResult<PyRef> {
    unsafe {
        PyRef::own(PyObject_Vectorcall(
            f,
            args.as_ptr(),
            args.len(),
            ptr::null_mut(),
        ))
    }
}

/// `f(*args[..npos], **dict(zip(kwnames, args[npos..])))`.
#[inline]
pub unsafe fn call_kw(
    f: *mut PyObject,
    args: &[*mut PyObject],
    npos: usize,
    kwnames: *mut PyObject,
) -> PResult<PyRef> {
    unsafe { PyRef::own(PyObject_Vectorcall(f, args.as_ptr(), npos, kwnames)) }
}

/// `args[0].name(*args[1..])`.
#[inline]
pub unsafe fn call_method(name: *mut PyObject, args: &[*mut PyObject]) -> PResult<PyRef> {
    unsafe {
        PyRef::own(PyObject_VectorcallMethod(
            name,
            args.as_ptr(),
            args.len(),
            ptr::null_mut(),
        ))
    }
}

pub unsafe fn getattr(o: *mut PyObject, name: &CStr) -> PResult<PyRef> {
    unsafe { PyRef::own(PyObject_GetAttrString(o, name.as_ptr())) }
}

pub unsafe fn import(name: &CStr) -> PResult<PyRef> {
    unsafe { PyRef::own(PyImport_ImportModule(name.as_ptr())) }
}

/// Runs `f` over the bytes of a `bytes`, `bytearray` or any buffer.
#[inline]
pub unsafe fn with_bytes<R>(o: *mut PyObject, f: impl FnOnce(&[u8]) -> R) -> PResult<R> {
    unsafe {
        if PyBytes_Check(o) != 0 {
            let p = PyBytes_AsString(o) as *const u8;
            let n = PyBytes_Size(o) as usize;
            return Ok(f(std::slice::from_raw_parts(p, n)));
        }
        let mut view = std::mem::MaybeUninit::<Py_buffer>::zeroed();
        if PyObject_GetBuffer(o, view.as_mut_ptr(), PyBUF_SIMPLE) < 0 {
            return Err(PyErr);
        }
        let mut view = view.assume_init();
        let slice = if view.len > 0 {
            std::slice::from_raw_parts(view.buf as *const u8, view.len as usize)
        } else {
            &[]
        };
        let r = f(slice);
        PyBuffer_Release(&mut view);
        Ok(r)
    }
}

/// UTF-8 view of a `str`; valid for as long as the object lives.
pub unsafe fn str_view<'a>(o: *mut PyObject) -> PResult<&'a [u8]> {
    unsafe {
        let mut n: Py_ssize_t = 0;
        let p = PyUnicode_AsUTF8AndSize(o, &mut n);
        if p.is_null() {
            return Err(PyErr);
        }
        Ok(std::slice::from_raw_parts(p as *const u8, n as usize))
    }
}

pub unsafe fn raise(exc: *mut PyObject, msg: &str) -> PyErr {
    let msg = CString::new(msg.replace('\0', "")).unwrap_or_default();
    unsafe { PyErr_SetString(exc, msg.as_ptr() as *const c_char) };
    PyErr
}

pub unsafe fn runtime_error(msg: &str) -> PyErr {
    unsafe { raise(PyExc_RuntimeError, msg) }
}

pub unsafe fn type_error(msg: &str) -> PyErr {
    unsafe { raise(PyExc_TypeError, msg) }
}

/// Takes the pending exception, normalised and with its traceback attached.
pub unsafe fn take_exception() -> Option<PyRef> {
    unsafe {
        #[cfg(Py_3_12)]
        {
            let e = PyErr_GetRaisedException();
            NonNull::new(e).map(PyRef)
        }
        #[cfg(not(Py_3_12))]
        {
            let (mut t, mut v, mut tb) = (ptr::null_mut(), ptr::null_mut(), ptr::null_mut());
            PyErr_Fetch(&mut t, &mut v, &mut tb);
            if t.is_null() {
                return None;
            }
            PyErr_NormalizeException(&mut t, &mut v, &mut tb);
            if !tb.is_null() && !v.is_null() {
                PyException_SetTraceback(v, tb);
            }
            Py_XDECREF(t);
            Py_XDECREF(tb);
            NonNull::new(v).map(PyRef)
        }
    }
}

/// Reports the pending exception without raising it anywhere: through
/// `report(exc)` when one is given, `sys.unraisablehook` otherwise.
pub unsafe fn report_exception(report: Option<*mut PyObject>) {
    unsafe {
        if PyErr_Occurred().is_null() {
            return;
        }
        match report {
            Some(f) => {
                let Some(exc) = take_exception() else { return };
                if call(f, &[exc.ptr()]).is_err() {
                    PyErr_WriteUnraisable(f);
                }
            }
            None => PyErr_WriteUnraisable(ptr::null_mut()),
        }
    }
}

pub unsafe fn f64_arg(o: *mut PyObject) -> PResult<f64> {
    unsafe {
        let v = PyFloat_AsDouble(o);
        if v == -1.0 && !PyErr_Occurred().is_null() {
            Err(PyErr)
        } else {
            Ok(v)
        }
    }
}

pub unsafe fn i64_arg(o: *mut PyObject) -> PResult<i64> {
    unsafe {
        let v = PyLong_AsLongLong(o);
        if v == -1 && !PyErr_Occurred().is_null() {
            Err(PyErr)
        } else {
            Ok(v)
        }
    }
}

pub unsafe fn truthy(o: *mut PyObject) -> PResult<bool> {
    match unsafe { PyObject_IsTrue(o) } {
        -1 => Err(PyErr),
        v => Ok(v != 0),
    }
}
