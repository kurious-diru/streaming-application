# My thought process while building the solution

### Ways to solve the problem:

- Build a basic API which takes input and has 3 endpoints wrt the given 3 tasks
- Build a streaming application using Faust and Kafka
- Build an application simulating the task at hand
- I am going with the simplest solution which does the task at hand (3rd)

### Other thoughts:

- The data is pre processed and clean to begin with
- The whole csv file shouldn't be taken into memory at once.


### Assumptions: 

- The order in which the data is processed is not taken into account
- As it is mimicking a streaming application, the input is given on one-by-one basis
- Limiting the calculated value precision to upto 3 decimal digits