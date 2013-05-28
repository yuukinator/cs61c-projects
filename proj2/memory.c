#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "memory.h"

/* Pointer to simulator memory */
uint8_t *mem;

/* Called by program loader to initialize memory. */
uint8_t *init_mem() {
  assert (mem == NULL);
  mem = calloc(MEM_SIZE, sizeof(uint8_t)); // allocate zeroed memory
  return mem;
}

/* Returns 1 if memory access is ok, otherwise 0 */
int access_ok(uint32_t mipsaddr, mem_unit_t size) {

    if (mipsaddr < 1 || mipsaddr > MEM_SIZE || mipsaddr == 0) {
	    return 0;
    }

    switch(size)
    {
    case SIZE_WORD:
	if ((mipsaddr % 4) != 0) {
	    return 0;
	    break;
	} else {
	    return 1;
	    break;
	}
    case SIZE_HALF_WORD:
	if ((mipsaddr % 2) != 0) {
	    return 0;
	    break;
	} else {
	    return 1;
	    break;
	}

    case SIZE_BYTE:
	return 1;
	
    default:
	return 1;
    }
}

/* Writes size bytes of value into mips memory at mipsaddr */
void store_mem(uint32_t mipsaddr, mem_unit_t size, uint32_t value) {
  if (!access_ok(mipsaddr, size)) {
    fprintf(stderr, "%s: bad write=%08x\n", __FUNCTION__, mipsaddr);
    exit(-1);
  } else if (size == SIZE_BYTE) {
      *(mem + mipsaddr) = value & 0x000000FF;
  } else if (size == SIZE_HALF_WORD) {
      *(mem + mipsaddr) = value & 0x000000FF;
      value = value >> 8;
      *(mem + mipsaddr + 1) = value & 0x00000FF;
  } else {
      *(mem + mipsaddr) = value & 0x000000FF;
      value = value >> 8;
      *(mem + mipsaddr + 1) = value & 0x000000FF;
      value = value >> 8;
      *(mem + mipsaddr + 2) = value & 0x000000FF;
      value = value >> 8;
      *(mem + mipsaddr + 3) = value & 0x000000FF;
  }
  
}

/* Returns zero-extended value from mips memory */
uint32_t load_mem(uint32_t mipsaddr, mem_unit_t size) {
  if (!access_ok(mipsaddr, size)) {
    fprintf(stderr, "%s: bad read=%08x\n", __FUNCTION__, mipsaddr);
    exit(-1);
  } else if (size == SIZE_WORD) {
      return *(uint32_t*)(mem + mipsaddr);
  } else if (size == SIZE_HALF_WORD) {
      return (uint32_t) (*(uint16_t*)(mem + mipsaddr));
  } else {
      return (uint32_t) (*(uint8_t*)(mem + mipsaddr));
  }
}
