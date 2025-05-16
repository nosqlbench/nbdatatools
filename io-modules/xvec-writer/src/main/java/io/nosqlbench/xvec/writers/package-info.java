/// # REQUIREMENTS
/// This package contains writers implementing [[io.nosqlbench.nbvectors.api.noncore.VectorStreamStore]] for ivec, and
/// fvec formats.
///
/// Each implementation of a VectorWriter in this package must do the following:
/// * Ensure that each vector written to the file is the same dimension. That is, after the first
///  one is written, the dimension of it should be remembered, and all later dimensions
/// should be checked against this.
/// * Each implementation should contain an explicit no-args constructor to support SPI.
/// * Each vector written should consist of a little-endian int dimension and a little-endian
/// buffer of values in the required format.
/// * There should be implementations for the following types:
///   * int[] as IvecVectorWriter
///   * float[] as FvecVectorWriter
///   * double[] as DvecVectorWriter
///   * short[] as SvecVectorWriter
///   * There should also be a special HvecVectorWriter which uses IEEE 754-2008 binary 16
/// encoding for JDK20 compatible 16-bit floating point types.
package io.nosqlbench.xvec.writers;