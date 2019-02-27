package jhi.brapi.client;

import java.util.List;
import java.util.Optional;
import jhi.brapi.api.Status;

public class AsyncChecker {
    public static final String ASYNCID = "asyncid";
    public static final String ASYNCSTATUS = "asyncstatus";
    public static final String ASYNC_PENDING = "PENDING";
    public static final String ASYNC_INPROCESS = "INPROCESS";
    public static final String ASYNC_FINISHED = "FINISHED";
    public static final String ASYNC_FAILED = "FAILED";

    public static Status hasAsyncId(List<Status> statuses) {
        Status status = null;
        Optional<Status> asyncStatus = statuses.stream().filter(s -> s.getCode().equalsIgnoreCase(ASYNCID) || s.getCode().equalsIgnoreCase("asynchid")).findFirst();
        if (asyncStatus.isPresent()) {
            status = asyncStatus.get();
        }
        return status;
    }

    public static AsyncStatus checkStatus(List<Status> statuses) {
        Status status = null;
        Optional<Status> asyncStatus = statuses.stream().filter(s -> s.getCode().equalsIgnoreCase(ASYNCSTATUS) || s.getCode().equalsIgnoreCase("asynchstatus")).findFirst();
        if (asyncStatus.isPresent()) {
            status = asyncStatus.get();
        }
        AsyncStatus found = AsyncStatus.UNKNOWN;
        if (AsyncChecker.callPending(status)) {
            found = AsyncStatus.PENDING;
        } else if (AsyncChecker.callInProcess(status)) {
            found = AsyncStatus.INPROCESS;
        } else if (AsyncChecker.callFinished(status)) {
            found = AsyncStatus.FINISHED;
        } else if (AsyncChecker.callFailed(status)) {
            found = AsyncStatus.FAILED;
        }
        return found;
    }

    public static Status checkAsyncStatus(List<Status> statuses) {
        Status status = null;
        Optional<Status> asyncStatus = statuses.stream().filter(s -> s.getCode().equalsIgnoreCase(ASYNCSTATUS) || s.getCode().equalsIgnoreCase("asynchstatus")).findFirst();
        if (asyncStatus.isPresent()) {
            status = asyncStatus.get();
        }
        return status;
    }

    public static boolean callPending(Status status) {
        return status != null && status.getMessage().equalsIgnoreCase(ASYNC_PENDING);
    }

    public static boolean callInProcess(Status status) {
        return status != null && status.getMessage().equalsIgnoreCase(ASYNC_INPROCESS);
    }

    public static boolean callFinished(Status status) {
        return status != null && status.getMessage().equalsIgnoreCase(ASYNC_FINISHED);
    }

    public static boolean callFailed(Status status) {
        return status != null && status.getMessage().equalsIgnoreCase(ASYNC_FAILED);
    }

    public static enum AsyncStatus {
        PENDING,
        INPROCESS,
        FINISHED,
        FAILED,
        UNKNOWN;
        

        private AsyncStatus() {
        }
    }

}

