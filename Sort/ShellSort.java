package wdh.sort;

public class ShellSort {

	public static void main(String[] args) {
		int [] array={4,2,3,6,5,1,9,7,8};
		int n =array.length;
		shellSort(array, n);
		for(int i : array){
			System.out.print(i);
		}
	}
	public static void shellSort(int[] array,int n){
		int i,j,gap;
		for(gap=n/2;gap>0;gap=gap/2){
			for(i=gap;i<n;i++){
				for(j=i;j>0;j--){
					if(array[j]>array[j-1]){
						int temp=array[j];
						array[j]=array[j-1];
						array[j-1]=temp;
					}
				}
			}
		}
	}
}
