/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by wangyunlai on 2021/6/11.
//

#include <string.h>
#include <algorithm>
#include "common/defs.h"

namespace common {


int compare_int(void *arg1, void *arg2)
{
  int v1 = *(int *)arg1;
  int v2 = *(int *)arg2;
  return v1 - v2;
}

int compare_date(void *arg1, void *arg2)
{
  return compare_int(arg1,arg2);
}

int compare_float(void *arg1, void *arg2)
{
  float v1 = *(float *)arg1;
  float v2 = *(float *)arg2;
  float cmp = v1 - v2;
  if (cmp > EPSILON) {
    return 1;
  }
  if (cmp < -EPSILON) {
    return -1;
  }
  return 0;
}

int compare_string(void *arg1, int arg1_max_length, void *arg2, int arg2_max_length)
{
  const char *s1 = (const char *)arg1;
  const char *s2 = (const char *)arg2;
  int maxlen = std::min(arg1_max_length, arg2_max_length);
  int result = strncmp(s1, s2, maxlen);
  if (0 != result) {
    return result;
  }

  if (arg1_max_length > maxlen) {
    return s1[maxlen] - 0;
  }

  if (arg2_max_length > maxlen) {
    return 0 - s2[maxlen];
  }
  return 0;
}

int compare_str_with_int(void *arg1,int arg_max_length,void *arg2){
  const char *s1 = (const char *)arg1;
  int v2 = *(int *)arg2;
  int maxlen = arg_max_length;
  int v3=0;
  char needle='+';
  for(int i=0;i<maxlen;i++){
    if(i==0){
      if(s1[i]=='+'){
          needle='+';
      }
      else if(s1[i]=='-'){
        needle='-';
      }
      else if((int)s1[i]>=48&&(int)s1[i]<=57){
        v3=v3*10;
        v3+=((int)s1[i]-48);
      }
      continue;
    }
    if((int)s1[i]>=48&&(int)s1[i]<=57){
      v3=v3*10;
      v3+=((int)s1[i]-48);
    }
    else{
      break;
    }
  }
  if(needle='-'){
    v3=-v3;
  }
  return v3-v2;
}

int compare_str_with_float(void *arg1,int agr1_max_length,void *agr2){
  const char *s1 = (const char *)arg1 ;
  float needle1=0.1;
  float v2 = *(float *)agr2;

  int maxlen = agr1_max_length;
  float v3=0;
  float v3_second=0;
  char needle='+';
  bool flag=true;
  for(int i=0;i<maxlen;i++){
    if(flag&&s1[i]=='.'){
      flag=false;
      continue;
    }
    if(i==0){
      if(s1[i]=='+'){
          needle='+';
      }
      else if(s1[i]=='-'){
        needle='-';
      }
      else if((int)s1[i]>=48&&(int)s1[i]<=57){
        v3=v3*10;
        v3+=((int)s1[i]-48);
      }
      continue;
    }
    if((int)s1[i]>=48&&(int)s1[i]<=57){
      if(flag){
        v3=v3*10;
        v3+=(float)((int)s1[i]-48);
      }
      else{
          float ss=(float)((int)s1[i]-48);
          v3+=ss*needle1;
          needle1/=10;
      }
    }
    else{
      break;
    }
  }
  if(needle='-'){
    v3=-v3;
  }
  float cmp = v3 - v2;
  if (cmp > EPSILON) {
    return 1;
  }
  if (cmp < -EPSILON) {
    return -1;
  }
  return 0;
}

} // namespace common
