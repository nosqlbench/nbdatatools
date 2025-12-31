# Vector Space Variates

## Synopsis

To simulate vector variates from a real or imagined space of vectors, this module will start with a basic model which approximates gaussian sampling from each component. While this is not a perfect model, it is a good starting point. Particularly, a model which only represents each component as a single statistical distribution is weak in that it will not be able to capture less uniform variances or clustering which may occur in any selected vector subspace. In fact, it is these variations which tend to make any vector dataset uniquely interesting.

Nonetheless, the ability to test extremely large virtual datasets is quite empowering, adding speed and flexibility to systems testing. Moving around bulky data slows things down and can complicate client-side apparatus. Once a basic model is in place, we can refine it better capture the real world.
 
## Basic Requirements

Basic requirements include:
* Continue tradition of O(1) sampling cost in keeping with other nosqlbench standards.
* Avoid unnecessary IO once the model is live in memory.
* Ensure stable and replayable vector sequences for a given set of parameters.
* Avoid artifacts of congruency or similar effects which can occur when applying uniform variate sampling as inputs to a multi-dimensional space.
* Use AVX-512 and other features when panama is available to the runtime, using multi-version jars.
* Keep the functions in this module relatively pure and dependency-free. Some usage of math libraries may be acceptable.
* Include elaborate unit tests, jmh tests, and javadoc to explain the bounds of performance and accuracy for new users and developers.

## Base Method 1 (permuted-stratified-sampling)

This method will incorporate a few functional (functional in the algebraic sense) building blocks, chained together in a unary flow, such that an input long represents a selected vector from the set of N possible vectors, and the output is an M-dimensional vector sampled from the M-dimensional space containing N distinct and non-repeating vectors.

The following parameters and functions will be stitched together to create the pipeline.

### The variate selector
Given the number of possible unique vector values, A user-provided function will select one of these by ordinal.

### The vector space model
A discrete model of the vector space will be provided by the user, consisting of
* The number of unique vectors N
* The dimensionality of the vector space M
* For each component dimension in the space, a basic gaussian model for the distribution of values, consisting of mean and standard deviation.

### The unit transform sampler
For each component of the vector, a basic gaussian sampler will be used to generate a value from the distribution. This will use inverse transform sampling, but with the customizations needed to ensure accuracy, given that the range is only supported under the domain of (-1.0,1.0) for the non-inverted form.

