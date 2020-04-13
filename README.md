# SimpleDB

## [Overview](https://ocw.mit.edu/courses/electrical-engineering-and-computer-science/6-830-database-systems-fall-2010/assignments/MIT6_830F10_overview.pdf)

### What is SimpleDB? 
- **A basic database system** 
- **What is has** 
    – Heapfiles 
    – Basic Operators (Scan, Filter, JOIN, Aggregate) 
    – Buffer Pool 
    – Transactions 
    – SQL Front­end 


- **Things it doesn’t have**
    – Query optimizer 
    – Fancy relational operators (UNION, etc) 
    – Recovery – Indices


![Fig: Module Diagram](https://paper-attachments.dropbox.com/s_532DC496C3353897D432233C2CB03E2C80C729AE483EB7F9EC058C0ED75B6542_1581317048762_Screen+Shot+2020-02-09+at+10.41.00+PM.png)

This project is implementation of a simple relational DB. The skeleton files and instructions are provided by MIT as part of 
**6.830: Database Systems** course. See [course page](https://ocw.mit.edu/courses/electrical-engineering-and-computer-science/6-830-database-systems-fall-2010).

## About the repo

- This repo is using a `gradle` build system instead of ant build provided in the course assignment files. I made it this way while setting up
  my intellij. I also did some repackaging, to follow with java project conventions I am familiar to.

- ~~I have also added `Uber/Nullaway` to check for potential bugs and added `goJF` java formatting tool (attached to git pre-commit hook)~~ [Decided to remove]

- There are 5 labs and each lab has bunch of exercises. For each lab, I have added a github project in this repo (lab1, lab2, lab3, lab4 and lab5) and each lab project has bunch of issues. Each issue corresponds to
  a exercise in the lab. Each issue is fixed by a commit (marking the issue done).

- For course readings and unified view of lab instructions, I'd recommend going to the [course page](https://ocw.mit.edu/courses/electrical-engineering-and-computer-science/6-830-database-systems-fall-2010/readings/)


## Other notes

- I am following the video lectures for `Database Systems` from  CMU database group: [youtube](https://www.youtube.com/watch?v=oeYBdghaIjc&list=PLSE8ODhjZXjbohkNBWQs_otTrBTrjyohi). That course uses C++ project;
  I am not spending time in C++. The syllabus looks similar to the MIT course, so opted for Java project instead.

- I read something about Databases from `Designing Data-Intensive Applications` by Martin Klepman, so I had some context while following the lectures and working on this project.

## Progress
- [X] lab 1: SimpleDB setup
- [X] lab 2: SimpleDB operators
- [X] lab 3: SimpleDB transactions 
- [ ] lab 4: Query optimization
- [ ] lab 5: Rollback and recovery
