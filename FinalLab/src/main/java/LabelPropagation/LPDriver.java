package LabelPropagation;

public class LPDriver {
    public static void main(String args[]) throws Exception{
        String[] forGB = {args[0], args[1] + "/Input"};
        InputInitial.main(forGB);

        String[] forItr = {forGB[1], args[1] + "/Output"};
        int ans = MyLabelPropagation.main(forItr);

        String[] forRV = {forItr[1] + ans, args[1] + "/FinalOutput"};
        OutputClean.main(forRV);


    }
}