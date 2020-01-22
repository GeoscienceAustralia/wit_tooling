import numpy as np

cimport numpy as np
cimport openmp

from cython.parallel import prange, parallel, threadid
from libc.stdlib cimport abort, malloc, free
from libc.math cimport fabs
from cython import boundscheck

ctypedef np.int64_t int64_t
ctypedef np.float64_t float64_t
ctypedef np.float32_t float32_t

@boundscheck(False)
def _cal_area(float32_t [:, :, :] data, int64_t [:, :] mask,
        int64_t [:] fid, float32_t [:] nodata, 
        float64_t [:, :] results, int64_t [:] vfid, int nthreads):

    cdef int row = data.shape[1]
    cdef int col = data.shape[2]
    cdef int var_size = data.shape[0]
    cdef int poly_num = fid.shape[0]
    cdef int idx, i, j, p, q
    cdef float64_t *poly_area
    cdef float64_t *valid_area

    poly_area = <float64_t *> malloc(sizeof(float64_t) * poly_num)
    valid_area = <float64_t *> malloc(sizeof(float64_t) * poly_num)

    for idx in range(poly_num):
        poly_area[idx] = 0
        valid_area[idx] = 0

    with nogil, parallel(num_threads=nthreads):
        for i in prange(poly_num):
            for p in range(row):
                for q in range(col):
                    if mask[p, q] != fid[i]:
                        continue

                    poly_area[i] = poly_area[i] + 1
                    if fabs(data[3, p, q] - nodata[3]) > 1E-14:
                        valid_area[i] = valid_area[i] + 1
                        if data[3, p, q] >= -350.:
                            results[i, 3] = results[i, 3] + 1
                        else: 
                            for j in range(var_size-2):
                                if fabs(data[j, p, q] - nodata[j]) > 1E-14:
                                    results[i, j] = results[i, j] + data[j, p, q]/100
                    if fabs(data[4, p, q] - nodata[4]) > 1E-14:
                        results[i, 4] = results[i, 4] + 1
                        valid_area[i] = valid_area[i] + 1

            if poly_area[i] < 1e-14:
                vfid[i] = -1
            elif valid_area[i]/poly_area[i] > 0.9:
                for j in range(var_size):
                    results[i, j] = results[i, j]/valid_area[i]     
                vfid[i] = fid[i]
            else:
                vfid[i] = -1
    free(poly_area)
    free(valid_area)

def cal_area(np.ndarray[float32_t, ndim=3] data, np.ndarray[int64_t, ndim=2] mask,
             np.ndarray[int64_t, ndim=1] fid, np.ndarray[float32_t, ndim=1] nodata, nthreads=None):
    var_size = data.shape[0]
    poly_num = fid.shape[0]
    results = np.zeros((poly_num, var_size), dtype=np.float64)
    vfid = np.zeros(poly_num, dtype=np.int64)

    if nthreads is None:
        nthreads = openmp.omp_get_max_threads()
    print("num procs", openmp.omp_get_num_procs())
    print("num threads", nthreads)
    _cal_area(data, mask, fid, nodata, results, vfid, nthreads)
    return results, vfid
