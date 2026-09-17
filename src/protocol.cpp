// SPDX-License-Identifier: MulanPSL-2.0
#include "protocol.h"
#include "data_path.h"

namespace rdma_bench {

void EncodeParams(uint8_t *&cursor, const CaseParameters &params)
{
    PutU16(cursor, params.version);
    PutU16(cursor, params.links);
    PutU32(cursor, params.blocks);
    PutU32(cursor, params.blockBytes);
    PutU32(cursor, params.strideBytes);
    PutU32(cursor, params.verifyRounds);
    PutU32(cursor, params.warmupRounds);
    PutU32(cursor, params.measureRounds);
    PutU32(cursor, params.traceRounds);
    PutU16(cursor, params.mode);
    PutU16(cursor, params.sglItems);
    PutU16(cursor, params.pipeline);
    PutU16(cursor, params.sourceFormat);
}

CaseParameters DecodeParams(const uint8_t *&cursor)
{
    CaseParameters params;
    params.version = GetU16(cursor);
    params.links = GetU16(cursor);
    params.blocks = GetU32(cursor);
    params.blockBytes = GetU32(cursor);
    params.strideBytes = GetU32(cursor);
    params.verifyRounds = GetU32(cursor);
    params.warmupRounds = GetU32(cursor);
    params.measureRounds = GetU32(cursor);
    params.traceRounds = GetU32(cursor);
    params.mode = GetU16(cursor);
    params.sglItems = GetU16(cursor);
    params.pipeline = GetU16(cursor);
    params.sourceFormat = GetU16(cursor);
    return params;
}

bool SameParams(const CaseParameters &left, const CaseParameters &right)
{
    return left.version == right.version && left.links == right.links && left.blocks == right.blocks &&
        left.blockBytes == right.blockBytes && left.strideBytes == right.strideBytes &&
        left.verifyRounds == right.verifyRounds && left.warmupRounds == right.warmupRounds &&
        left.measureRounds == right.measureRounds && left.traceRounds == right.traceRounds &&
        left.mode == right.mode && left.sglItems == right.sglItems && left.pipeline == right.pipeline &&
        left.sourceFormat == right.sourceFormat;
}

bool IsZeroMemoryKey(const UBSHcomMemoryKey &key)
{
    const UBSHcomMemoryKey zero{};
    return std::memcmp(&key, &zero, sizeof(key)) == 0;
}

void EncodeMemoryKey(uint8_t *&cursor, const UBSHcomMemoryKey &key)
{
    for (uint64_t value : key.keys) {
        PutU64(cursor, value);
    }
    for (uint64_t value : key.tokens) {
        PutU64(cursor, value);
    }
    std::memcpy(cursor, key.eid, sizeof(key.eid));
    cursor += sizeof(key.eid);
}

UBSHcomMemoryKey DecodeMemoryKey(const uint8_t *&cursor)
{
    UBSHcomMemoryKey key{};
    for (uint64_t &value : key.keys) {
        value = GetU64(cursor);
    }
    for (uint64_t &value : key.tokens) {
        value = GetU64(cursor);
    }
    std::memcpy(key.eid, cursor, sizeof(key.eid));
    cursor += sizeof(key.eid);
    return key;
}

std::array<uint8_t, kHelloWireBytes> EncodeHello(const HelloInfo &info)
{
    std::array<uint8_t, kHelloWireBytes> payload{};
    uint8_t *cursor = payload.data();
    PutU32(cursor, kHelloMagic);
    EncodeParams(cursor, info.params);
    PutU16(cursor, info.rail);
    PutU16(cursor, 0);
    PutU32(cursor, info.destinationRegionId);
    PutU32(cursor, 0);
    PutU64(cursor, info.destinationAddress);
    PutU64(cursor, info.destinationBytes);
    EncodeMemoryKey(cursor, info.destinationKey);
    PutU32(cursor, info.stageRegionId);
    PutU32(cursor, 0);
    PutU64(cursor, info.stageAddress);
    PutU64(cursor, info.stageBytes);
    EncodeMemoryKey(cursor, info.stageKey);
    if (cursor != payload.data() + payload.size())
        throw std::logic_error("HELLO encoder cursor does not match the wire extent");
    return payload;
}

bool DecodeHello(const void *data, uint32_t size, HelloInfo &info)
{
    if (data == nullptr || size != kHelloWireBytes) {
        return false;
    }
    const uint8_t *cursor = static_cast<const uint8_t *>(data);
    if (GetU32(cursor) != kHelloMagic) {
        return false;
    }
    info.params = DecodeParams(cursor);
    info.rail = GetU16(cursor);
    if (GetU16(cursor) != 0) {
        return false;
    }
    info.destinationRegionId = GetU32(cursor);
    if (GetU32(cursor) != 0) {
        return false;
    }
    info.destinationAddress = GetU64(cursor);
    info.destinationBytes = GetU64(cursor);
    info.destinationKey = DecodeMemoryKey(cursor);
    info.stageRegionId = GetU32(cursor);
    if (GetU32(cursor) != 0) {
        return false;
    }
    info.stageAddress = GetU64(cursor);
    info.stageBytes = GetU64(cursor);
    info.stageKey = DecodeMemoryKey(cursor);
    return cursor == static_cast<const uint8_t *>(data) + size;
}

std::array<uint8_t, kReadyWireBytes> EncodeReady(const ReadyInfo &info)
{
    std::array<uint8_t, kReadyWireBytes> payload{};
    uint8_t *cursor = payload.data();
    PutU32(cursor, kReadyMagic);
    EncodeParams(cursor, info.params);
    PutU16(cursor, info.rail);
    PutU16(cursor, 0);
    PutU32(cursor, info.sourceRegionId);
    PutU32(cursor, info.sourceAlignment);
    PutU64(cursor, info.sourceBytes);
    return payload;
}

bool DecodeReady(const void *data, uint32_t size, ReadyInfo &info)
{
    if (data == nullptr || size != kReadyWireBytes) {
        return false;
    }
    const uint8_t *cursor = static_cast<const uint8_t *>(data);
    if (GetU32(cursor) != kReadyMagic) {
        return false;
    }
    info.params = DecodeParams(cursor);
    info.rail = GetU16(cursor);
    if (GetU16(cursor) != 0) {
        return false;
    }
    info.sourceRegionId = GetU32(cursor);
    info.sourceAlignment = GetU32(cursor);
    info.sourceBytes = GetU64(cursor);
    return true;
}

void EncodeCopyRequest(uint64_t generation, const CaseParameters &params,
    const std::vector<CopyEntry> &entries, uint8_t *payload)
{
    uint8_t *cursor = payload;
    PutU32(cursor, kCopyReqMagic);
    PutU16(cursor, kProtocolVersion);
    PutU16(cursor, kOpCopyReq);
    PutU64(cursor, generation);
    PutU32(cursor, params.blocks);
    PutU32(cursor, params.blocks);
    PutU32(cursor, params.blockBytes);
    PutU16(cursor, params.mode);
    PutU16(cursor, params.links);
    PutU16(cursor, params.sglItems);
    PutU16(cursor, params.sourceFormat);
    PutU32(cursor, kSourceRegionId);
    PutU32(cursor, kDestinationRegionId);
    PutU32(cursor, params.mode == kModeSgl ? kStageRegionId : 0);
    PutU32(cursor, kCopyReqHeaderBytes);
    PutU32(cursor, params.blocks * kCopyEntryWireBytes);
    PutU32(cursor, static_cast<uint32_t>(params.PayloadBytes()));
    PutU32(cursor, 0);
    for (const CopyEntry &entry : entries) {
        PutU64(cursor, entry.remoteSourceOffset);
        PutU64(cursor, entry.localDestinationOffset);
    }
    if (cursor != payload + params.RequestBytes()) throw std::logic_error("COPY_REQ encoder extent");
}

bool DecodeCopyRequest(const void *data, uint32_t size, uint64_t expectedGeneration, const CaseParameters &params,
    uint64_t sourceBytesPerRail, uint64_t destinationBytesPerRail,
    std::vector<CopyEntry> &entries, std::string &error)
{
    const auto fail = [&error](const std::string &message) { error = message; return false; };
    if (data == nullptr || size != params.RequestBytes() || entries.size() != params.blocks)
        return fail("COPY_REQ length/count mismatch");
    const uint8_t *cursor = static_cast<const uint8_t *>(data);
    if (GetU32(cursor) != kCopyReqMagic || GetU16(cursor) != kProtocolVersion || GetU16(cursor) != kOpCopyReq ||
        GetU64(cursor) != expectedGeneration || GetU32(cursor) != params.blocks ||
        GetU32(cursor) != params.blocks || GetU32(cursor) != params.blockBytes ||
        GetU16(cursor) != params.mode || GetU16(cursor) != params.links ||
        GetU16(cursor) != params.sglItems || GetU16(cursor) != params.sourceFormat ||
        GetU32(cursor) != kSourceRegionId || GetU32(cursor) != kDestinationRegionId ||
        GetU32(cursor) != (params.mode == kModeSgl ? kStageRegionId : 0) || GetU32(cursor) != kCopyReqHeaderBytes ||
        GetU32(cursor) != params.blocks * kCopyEntryWireBytes || GetU32(cursor) != params.PayloadBytes() ||
        GetU32(cursor) != 0) return fail("COPY_REQ fields differ from negotiated case");
    for (CopyEntry &entry : entries) {
        entry.remoteSourceOffset = GetU64(cursor);
        entry.localDestinationOffset = GetU64(cursor);
    }
    return cursor == static_cast<const uint8_t *>(data) + size &&
        ValidateCopyEntries(entries, params, sourceBytesPerRail, destinationBytesPerRail, error);
}

uint32_t EncodeRequestFragment(uint64_t generation, uint32_t total, uint32_t offset,
    const uint8_t *request, uint8_t *fragment)
{
    if (total > kMaxCopyReqWireBytes || offset >= total || offset % kFragmentDataBytes != 0)
        throw std::logic_error("invalid request fragment extent");
    const uint32_t bytes = std::min(kFragmentDataBytes, total - offset);
    uint8_t *cursor = fragment;
    PutU32(cursor, kFragmentMagic);
    PutU16(cursor, kProtocolVersion);
    PutU16(cursor, 0);
    PutU64(cursor, generation);
    PutU32(cursor, total);
    PutU32(cursor, offset);
    PutU32(cursor, bytes);
    PutU32(cursor, 0);
    std::memcpy(cursor, request + offset, bytes);
    return kFragmentHeaderBytes + bytes;
}

bool AppendRequestFragment(const void *data, uint32_t size, uint64_t generation,
    const CaseParameters &params, uint8_t *request, uint32_t &received)
{
    if (data == nullptr || size <= kFragmentHeaderBytes || size > kFragmentWireBytes) return false;
    const uint8_t *cursor = static_cast<const uint8_t *>(data);
    if (GetU32(cursor) != kFragmentMagic || GetU16(cursor) != kProtocolVersion || GetU16(cursor) != 0 ||
        GetU64(cursor) != generation) return false;
    const uint32_t total = GetU32(cursor);
    const uint32_t offset = GetU32(cursor);
    const uint32_t bytes = GetU32(cursor);
    if (GetU32(cursor) != 0 || total != params.RequestBytes() || total > kMaxCopyReqWireBytes ||
        offset != received || offset >= total || offset % kFragmentDataBytes != 0 ||
        bytes != std::min(kFragmentDataBytes, total - offset) || size != kFragmentHeaderBytes + bytes) return false;
    std::memcpy(request + offset, cursor, bytes);
    received += bytes;
    return true;
}

std::array<uint8_t, kChunkDoneWireBytes> EncodeChunkDone(const ChunkDoneInfo &info)
{
    std::array<uint8_t, kChunkDoneWireBytes> payload{};
    uint8_t *cursor = payload.data();
    PutU32(cursor, kChunkDoneMagic);
    PutU16(cursor, kProtocolVersion);
    PutU16(cursor, info.rail);
    PutU64(cursor, info.generation);
    PutU32(cursor, info.chunkId);
    PutU32(cursor, info.firstItem);
    PutU32(cursor, info.itemCount);
    PutU32(cursor, info.payloadBytes);
    PutU32(cursor, info.chunkCount);
    PutU32(cursor, 0);
    return payload;
}

bool DecodeChunkDone(const void *data, uint32_t size, ChunkDoneInfo &info)
{
    if (data == nullptr || size != kChunkDoneWireBytes) return false;
    const uint8_t *cursor = static_cast<const uint8_t *>(data);
    if (GetU32(cursor) != kChunkDoneMagic || GetU16(cursor) != kProtocolVersion) return false;
    info.rail = GetU16(cursor);
    info.generation = GetU64(cursor);
    info.chunkId = GetU32(cursor);
    info.firstItem = GetU32(cursor);
    info.itemCount = GetU32(cursor);
    info.payloadBytes = GetU32(cursor);
    info.chunkCount = GetU32(cursor);
    return GetU32(cursor) == 0;
}

bool ValidateChunkDone(const ChunkDoneInfo &info, uint16_t expectedRail, uint64_t expectedGeneration,
    uint32_t blocksPerRail, uint16_t sglItems, uint32_t blockBytes, std::string &error)
{
    const uint32_t chunks = ChunkCount(blocksPerRail, sglItems);
    const uint32_t count = ChunkItemCount(blocksPerRail, sglItems, info.chunkId);
    const uint64_t first = static_cast<uint64_t>(info.chunkId) * sglItems;
    if (info.rail != expectedRail || info.generation != expectedGeneration || info.generation == 0 ||
        info.chunkId >= chunks || count == 0 || info.firstItem != first || info.itemCount != count ||
        info.payloadBytes != count * blockBytes || info.chunkCount != chunks) {
        error = "CHUNK_DONE fields do not match the current generation/chunk layout";
        return false;
    }
    return true;
}

std::array<uint8_t, kDataDoneWireBytes> EncodeDataDone(uint64_t generation, uint16_t rail,
    uint32_t blocksOnRail, uint32_t blockBytes)
{
    std::array<uint8_t, kDataDoneWireBytes> payload{};
    uint8_t *cursor = payload.data();
    PutU32(cursor, kDataDoneMagic);
    PutU16(cursor, kProtocolVersion);
    PutU16(cursor, kOpDataDone);
    PutU64(cursor, generation);
    PutU16(cursor, rail);
    PutU16(cursor, 0);
    PutU32(cursor, blocksOnRail);
    PutU32(cursor, blocksOnRail * blockBytes);
    PutU32(cursor, 0);
    return payload;
}

bool DecodeDataDone(const void *data, uint32_t size, uint16_t expectedRail, uint32_t expectedBlocks,
    uint64_t &generation, uint32_t blockBytes)
{
    if (data == nullptr || size != kDataDoneWireBytes) {
        return false;
    }
    const uint8_t *cursor = static_cast<const uint8_t *>(data);
    return GetU32(cursor) == kDataDoneMagic && GetU16(cursor) == kProtocolVersion &&
        GetU16(cursor) == kOpDataDone && (generation = GetU64(cursor)) != 0 &&
        GetU16(cursor) == expectedRail && GetU16(cursor) == 0 &&
        GetU32(cursor) == expectedBlocks && GetU32(cursor) == expectedBlocks * blockBytes &&
        GetU32(cursor) == 0;
}

std::array<uint8_t, kCopyErrorWireBytes> EncodeCopyError(
    uint64_t generation, uint32_t stage, uint32_t errorCode, uint32_t detail)
{
    std::array<uint8_t, kCopyErrorWireBytes> payload{};
    uint8_t *cursor = payload.data();
    PutU32(cursor, kCopyErrorMagic);
    PutU16(cursor, kProtocolVersion);
    PutU16(cursor, kOpCopyError);
    PutU64(cursor, generation);
    PutU32(cursor, stage);
    PutU32(cursor, errorCode);
    PutU32(cursor, detail);
    PutU32(cursor, 0);
    return payload;
}

bool DecodeCopyError(const void *data, uint32_t size, uint64_t &generation,
    uint32_t &stage, uint32_t &errorCode, uint32_t &detail)
{
    if (data == nullptr || size != kCopyErrorWireBytes) {
        return false;
    }
    const uint8_t *cursor = static_cast<const uint8_t *>(data);
    if (GetU32(cursor) != kCopyErrorMagic || GetU16(cursor) != kProtocolVersion ||
        GetU16(cursor) != kOpCopyError) {
        return false;
    }
    generation = GetU64(cursor);
    stage = GetU32(cursor);
    errorCode = GetU32(cursor);
    detail = GetU32(cursor);
    return GetU32(cursor) == 0;
}

std::array<uint8_t, kTokenWireBytes> EncodeToken(
    uint32_t magic, uint16_t opcode, uint64_t generation, uint16_t rail)
{
    std::array<uint8_t, kTokenWireBytes> payload{};
    uint8_t *cursor = payload.data();
    PutU32(cursor, magic);
    PutU16(cursor, kProtocolVersion);
    PutU16(cursor, opcode);
    PutU64(cursor, generation);
    PutU16(cursor, rail);
    PutU16(cursor, 0);
    PutU32(cursor, 0);
    return payload;
}

bool DecodeToken(const void *data, uint32_t size, uint32_t magic, uint16_t opcode,
    uint64_t &generation, uint16_t &rail)
{
    if (data == nullptr || size != kTokenWireBytes) {
        return false;
    }
    const uint8_t *cursor = static_cast<const uint8_t *>(data);
    if (GetU32(cursor) != magic || GetU16(cursor) != kProtocolVersion || GetU16(cursor) != opcode) {
        return false;
    }
    generation = GetU64(cursor);
    rail = GetU16(cursor);
    return GetU16(cursor) == 0 && GetU32(cursor) == 0;
}

bool ValidateHelloMetadata(const HelloInfo &hello, const CaseParameters &parameters, uint16_t rail,
    uint64_t railBytes, uint64_t stageBytes)
{
    uint64_t ignored = 0;
    if (!SameParams(hello.params, parameters) || hello.rail != rail ||
        hello.destinationRegionId != kDestinationRegionId || hello.destinationAddress == 0 ||
        hello.destinationAddress > std::numeric_limits<uintptr_t>::max() || hello.destinationBytes != railBytes ||
        !CheckedAddAddress(hello.destinationAddress, 0, railBytes, railBytes, ignored)) return false;
    if (parameters.mode == kModeDirect) {
        return hello.stageRegionId == 0 && hello.stageAddress == 0 && hello.stageBytes == 0 &&
            IsZeroMemoryKey(hello.stageKey);
    }
    return parameters.mode == kModeSgl && hello.stageRegionId == kStageRegionId && hello.stageAddress != 0 &&
        hello.stageAddress <= std::numeric_limits<uintptr_t>::max() && hello.stageBytes == stageBytes &&
        CheckedAddAddress(hello.stageAddress, 0, stageBytes, stageBytes, ignored);
}

}  // namespace rdma_bench
