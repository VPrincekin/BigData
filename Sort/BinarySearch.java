package wdh.sort;

public class BinarySearch {
	/**二分查找
	 * log2n
	 * */
	public static void main(String[] args) {
		int[] array={1,2,5,6,3,4,9,8,7};
		int[] bubbleSort = BubbleSort(array);
		for(int i:bubbleSort){
			System.out.print(i);
		}
		System.out.println();
		int t = binarySearch(bubbleSort, 0, bubbleSort.length-1, 6);
		System.out.println(t);
		int tt=binarySearch2(bubbleSort, 0, bubbleSort.length-1, 5);
		System.out.println(tt);
	}
	public static int[] BubbleSort(int[] array){
		int n=array.length;
		int temp;
		for(int i=0;i<n;i++){
			for(int j=0;j<n-i-1;j++){
				if(array[j]>array[j+1]){
					temp=array[j];
					array[j]=array[j+1];
					array[j+1]=temp;
				}
			}
		}
		return array;
	}
	public static int binarySearch(int[] array,int low,int high,int desc){
		int mid=(low+high)/2;
		if(array[mid]>desc){
			return binarySearch(array, low, mid-1, desc);
		}
		if(array[mid]<desc){
			return binarySearch(array, mid+1, high, desc);
		}
		return mid;
	}
	
	public static int binarySearch2(int[] array,int low,int high,int desc){
		while(low<high){
			int mid=(low+high)/2;
			if(array[mid]>desc){
				high=mid-1;
			}
			else if(array[mid]<desc){
				low=mid+1;
			}else
			return mid;
		}
		return -1;
	}
}
