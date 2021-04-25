# My thought process while building the solution

### Ways to solve the problem:

1. Build a basic API which takes input and has 3 endpoints with respect to the given 3 tasks
2. Build a streaming application using Faust and Kafka
3. Build an application simulating the task at hand
- I am going with the simplest solution which does the task at hand (3rd)

### Other thoughts:

- The data is pre processed and clean to begin with
- The whole csv file shouldn't be taken into memory at once.


### Assumptions: 

- The processing order of input data is not maintained
- As it is mimicking a streaming application, the input is given one record at a time
- Limiting the calculated value precision to upto 3 decimal digits
- The given data is clean and can be used directly

### Future scope:

- Write more tests to cover the application as it grows
- Include technologies which handle data in scale like Kafka streams and Faust
- Build a pre-processing pipeline to handle noisy data
- Can use a load balancing technology like Tremor if logs are too high
- 