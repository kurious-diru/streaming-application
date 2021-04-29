# My thought process while building the solution

### Ways to solve the problem:

1. Build a basic API which takes input and has 3 endpoints with respect to the given 3 tasks
2. Build a streaming application using Faust and Kafka
3. Build an application simulating the task at hand
- I am going with the simplest solution which does the task at hand (3rd)

### Other thoughts/Design decisions:

- The data is pre processed and clean to begin with
- The whole csv file shouldn't be taken into memory at once
- Since there is no lag in appending data, the processors do not take advantage of concurrency
- The tests are written modularly which can be extended in future

### Assumptions: 

- The processing order of input data is not maintained
- As it is mimicking a streaming application, the input is given one record at a time
- Limiting the calculated value precision to upto 3 decimal digits
- The given data is clean and can be used directly

### Future scope / Potential Improvements:

- Write more tests to cover the application as it grows
- Build a pre-processing pipeline to handle (if any) noisy data in future
- Add error handling
- Add other type of testing in future if needed(integration and e2edev)
- Add security to the handle data via encryption.
- Include technologies/frameworks which handle data in scale like Kafka streams and Faust(for python)
