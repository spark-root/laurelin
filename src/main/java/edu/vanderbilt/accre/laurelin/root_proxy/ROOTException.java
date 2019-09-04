package edu.vanderbilt.accre.laurelin.root_proxy;

public class ROOTException extends RuntimeException {
    public ROOTException(String string) {
        super(string);
    }

    private static final long serialVersionUID = 1L;

    public static class UnsupportedBranchTypeException extends ROOTException {
        public UnsupportedBranchTypeException(String string) {
            super(string);
        }

        private static final long serialVersionUID = 1L;
    }
}