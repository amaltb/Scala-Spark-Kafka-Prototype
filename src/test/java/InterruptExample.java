public class InterruptExample {

    class ThreadS1 implements Runnable {
        @Override
        public void run() {
            while (true)
            {
                System.out.println("Running thread: " + Thread.currentThread().getId());

                if(Thread.interrupted())
                {
                    System.out.println("inside if");
                    break;
                }
            }
        }
    }

    void run()
    {
        Thread t1 = new Thread(new ThreadS1());

        try{
            t1.start();

            Thread.sleep(5000);
        }
        catch (Exception e) {
        } finally {
            System.out.println("stopping..." + Thread.currentThread().getId());
            t1.interrupt();
        }


    }

    public static void main(String[] args) {

        InterruptExample sample = new InterruptExample();
        sample.run();
    }
}
