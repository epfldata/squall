package ch.epfl.data.squall.thetajoin.matrix_assignment.exp;
import java.util.concurrent.TimeoutException;


public final class TimeoutController {

    /**
     * Do not instantiate objects of this class. Methods are static.
     */
    private TimeoutController() {
    }

    /**
     * Executes <code>task</code>. Waits for <code>timeout</code>
     * milliseconds for the task to end and returns. If the task does not return
     * in time, the thread is interrupted and an Exception is thrown.
     * The caller should override the Thread.interrupt() method to something that
     * quickly makes the thread die or use Thread.isInterrupted().
     * @param task The thread to execute
     * @param timeout The timeout in milliseconds. 0 means to wait forever.
     * @throws java.util.concurrent.TimeoutException if the timeout passes and the thread does not return.
     */
    public static void execute(Thread task, long timeout) throws TimeoutException {
        task.start();
        try {
            task.join(timeout);
        } catch (InterruptedException e) {
            /* if somebody interrupts us he knows what he is doing */
        }
        if (task.isAlive()) {
            task.interrupt();
            throw new TimeoutException();
        }
    }

    /**
     * Executes <code>task</code> in a new deamon Thread and waits for the timeout.
     * @param task The task to execute
     * @param timeout The timeout in milliseconds. 0 means to wait forever.
     * @throws java.util.concurrent.TimeoutException if the timeout passes and the thread does not return.
     */
    public static void execute(Runnable task, long timeout) throws TimeoutException {
        Thread t = new Thread(task, "Timeout guard");
        t.setDaemon(true);
        execute(t, timeout);
    }

//    /**
//     * Signals that the task timed out.
//     */
//    public static class TimeoutException extends Exception {
//        /** Create an instance */
//        public TimeoutException() {
//        }
//    }
}
