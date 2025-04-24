/// # REQUIREMENTS
///
/// The commands in this package do the following:
///
/// ## FvecExtract (fvec-extract)
///
/// fvec-extract uses an indices ivec file to extract referent values from an input fvec file and
///  write them to an output fvec file.
/// - It does no pre-scanning before loading, and does not buffer large data into memory. It uses
///   random access features of the readers below instead.
/// - It does basic range checking by determining the record size and then extrapolating the
/// number of records based on the file size.
/// - fvec-extract extracts a sequence of values from an input fvec file
///   and writes them to an fvec file, using indices from a provide ivec
///   file which contains the positions to extract.
/// - It uses IvecReader and FvecReader and FvecWriter internally, and does no direct IO of its own.
/// - It provides a progress indicator on stdout.
/// - It allows the user to specific which indices will be used by providing a range.
/// - Once the IvecReader is opened, the minimum and maximum range values are accessed to assert
/// a valid range. This ensures a fail-fast check occurs before a long process that might fail
/// later.
///
///
/// ## IvecShuffle (ivec-shuffle)
///
/// ivec-shuffle creates a shuffled sequence of values and writes them to an ivec file.
/// - The user can specify the seed to use for the random number generator.
/// - The user can specify the RNG implementation for a set of supported options.
/// - The user can specify how many values should be generated.
/// - The user can specify the output file name.
package io.nosqlbench.nbvectors.commands.generate.commands;