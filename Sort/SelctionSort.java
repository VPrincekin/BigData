package wdh.sort;
public class SelctionSort {

	public static void main(String[] args) {
		int[] array={9,4,2,5,6,3,7,1};
		int[] sort = sort(array);
		for(int i:sort){
			System.out.print(i);
		}
	}
	public static int[] sort(int[] array){
		int n=array.length;
		for(int i=0;i<n;i++){
			int min=i;
			for(int j=i;j<n;j++){
				if(array[j]<array[min]){
					min=j;
				}
			}
			int temp=array[i];
			array[i]=array[min];
			array[min]=temp;
			
		}
		return array;
	}
}
