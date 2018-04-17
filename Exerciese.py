##########################################################################
# 1. two sum
# solution 1
class Solution(object):
    def twoSum(self,arr,targ):
        for m,x in enumerate(arr):
            for n,y in enumerate(arr):
                if arr[m] == targ - arr[n] and m!=n:
                    return  [m,n]
                    break

# solution 2
class Solution(object):
    def twoSum(self,arr,targ):
        for m,x in enumerate(arr):
            y = targ - arr[m]
            if y in arr and m!=arr.index(y):
                return  [m,arr.index(y)]
                break


##########################################################################
# 7. Reverse Integer
# solution 1 (didn't pass)
class Solution(object):
    def reverse(self,s):
        rev = ""
        if s < -(2**31) or s > 2**31 - 1:
            rev = 0
        else:
            if s < 0:
                s=str(s*-1)
                for i in s:
                    rev = i + rev
                rev=int(rev)*-1
            else:
                s=str(s)
                for i in s:
                    rev = i + rev
                rev=int(rev)
        return rev

# solution 2
class Solution(object):
    def reverse(self, x):
        s = cmp(x,0)
        r = int(str(x*s)[::-1])
        return s*r * (r < 2**31)


##########################################################################
# 9. Palindrome Number
class Solution(object):
    def isPalindrome(self,x):
        if x < 0:
            return False
        else:
            if x - int(str(x)[::-1]) == 0:
                return True
            else:
                return False


##########################################################################
# 13. Roman to Integer
class Solution(object):
    def romanToInt(self, s):
            """
            :type s: str
            :rtype: int
            """
            _dict = {'I':1,'V':5,'X':10,'L':50,'C':100,'D':500,'M':1000}
            prev = 0
            sum = 0
            for i in s[::-1]:
                curr = _dict[i]
                if prev > curr:
                    sum -= curr
                else:
                    sum += curr
                prev = curr
            return sum




##########################################################################
# 14. Longest Common Prefix
class Solution(object):
    def longestCommonPrefix(self, strs):
        """
        :type strs: List[str]
        :rtype: str
        """
        if not strs:
            return ""
        shortest = min(strs,key=len)
        for i, ch in enumerate(shortest):
            for other in strs:
                if other[i] != ch:
                    return shortest[:i]
        return shortest 


##########################################################################
# 20. Valid Parentheses
class Solution(object):
    def isValid(self, s):
        stack = []
        dict = {"]":"[", "}":"{", ")":"("}
        for char in s:
            if char in dict.values():
                stack.append(char)
            elif char in dict.keys():
                if stack == [] or dict[char] != stack.pop():
                    return False
            else:
                return False
        return stack == []



##########################################################################
# 21. Merge Two Sorted Lists
class Solution(object):
    def mergeTwoLists(self, l1, l2):
        dummy = cur = ListNode(0)
        while l1 and l2:
            if l1.val < l2.val:
                cur.next = l1
                l1 = l1.next
            else:
                cur.next = l2
                l2 = l2.next
            cur = cur.next
        cur.next = l1 or l2
        return dummy.next


##########################################################################
#26. Remove Duplicates from Sorted Array
class Solution(object):
    def removeDuplicates(self,A):
        if not A:
            return 0 
        newTail=0
        for i in range(1, len(A)):
            if A[i] != A[newTail]:
                newTail += 1
                A[newTail] = A[i]
        return newTail + 1
    

##########################################################################
#27. Remove Element
class Solution:
    def removeElement(self, nums, val): 
        while val in nums:
            nums.remove(val)


##########################################################################
#28. Implement strStr()
class Solution:
    def strStr(self, haystack, needle):
        try:
            return(haystack.index(needle))
        except:
            return(-1)


##########################################################################
#35. Search Insert Position
class Solution:
    def searchInsert(self, nums, target):
        for i in nums:
            if i >= target:
                return nums.index(i)
        return len(nums)



##########################################################################
#53. Maximum Subarray
class Solution:
    def maxSubArray(self, nums):
        for i in nums:
            if i < 0:
                nums.remove(i)
        return sum(nums)
