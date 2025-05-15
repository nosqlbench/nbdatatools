module xvec.writer.test {
    requires xvec.writer;
    requires testdata.apis;
    requires org.junit.jupiter.api;

    // Open the test package to JUnit for reflection
    opens io.nosqlbench.writers to org.junit.platform.commons;
}
