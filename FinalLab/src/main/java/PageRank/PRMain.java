package PageRank;

public class PRMain {
    private static int times = 10;
    public PRMain(){

    }
    public static void main(String[] args) throws Exception{
        try{
            String[] forPR = {"",""};
            forPR[0]=args[0];
            forPR[1]=args[1]+"/Data0";
            Initialize.main(forPR);//Initialize

            String[] forItr={"Data","Data"};
            for(int i=0;i<times;i++){
                forItr[0]=args[1]+"/Data"+(i);
                forItr[1]=args[1]+"/Data"+(i+1);
                PageRank.main(forItr);
            }//Iteration

            String[] forFS={args[1]+"/Data"+times,args[1]+"/FinalRank"};
            FinalSort.main(forFS);
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
}
