package wdh.sort;

public class BubbleSort{
	/**冒泡排序
	 *最好 O(n)
	 *最坏 O(n^2)
	 * */
	public static void main(String[] args){
		int[] array = {5,6,4,7,8,1,2,3,9};
		int[] array1 = sort(array);
		for(int i:array1){
			System.out.print(i);
		}
	}


	public static int[] sort(int[] array){
		int n = array.length;
		for(int i=0;i<n;i++){
			for(int j=0;j<n-i-1;j++){
				if(array[j]>array[j+1]){
					int temp=array[j];
					array[j]=array[j+1];
					array[j+1]=temp;
				}
			}
		}
		return array;
	}
}
