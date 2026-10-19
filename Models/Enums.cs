namespace RaftCore.Models;

public enum NodeRole { Discovery, Follower, Candidate, Leader }

public enum LogEntryType { NoOp = 0, Data = 1 }
