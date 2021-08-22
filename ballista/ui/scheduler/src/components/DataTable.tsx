// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import React from "react";
import {
  Link,
  Table,
  Thead,
  Flex,
  Tbody,
  Text,
  Tr,
  Th,
  Td,
  VStack,
  chakra,
} from "@chakra-ui/react";
import { TriangleDownIcon, TriangleUpIcon } from "@chakra-ui/icons";
import {
  useTable,
  useSortBy,
  usePagination,
  Column as RTColumn,
} from "react-table";
import { HiChevronLeft, HiChevronRight } from "react-icons/all";
import TimeAgo from "react-timeago";

type RenderFn = (props: any) => React.ReactNode;

interface Row {
  [name: string]: any;
}

// eslint-disable-next-line
export type Column<Row> =
  | RTColumn
  | {
      isNumeric?: boolean;
      render?: RenderFn;
    };

interface DataTableProps {
  columns: Column<Row>[];
  data: Row[];
  pageSize?: number;
  maxW?: number;
  pb?: number;
}

export const ElapsedCell: (props: any) => React.ReactNode = (props: any) => {
  const time = new Date(new Date().getTime() - props.value);
  return (
    <TimeAgo
      date={time}
      formatter={(
        value: number,
        unit: TimeAgo.Unit,
        suffix: TimeAgo.Suffix
      ) => {
        if (unit === "second") return "just now";
        const plural: string = value !== 1 ? "s" : "";
        return `${value} ${unit}${plural} ${suffix}`;
      }}
    />
  );
};

export const DateCell: (props: any) => React.ReactNode = (props: any) => {
  return (
    <TimeAgo
      minPeriod={60}
      date={props.value}
      formatter={(
        value: number,
        unit: TimeAgo.Unit,
        suffix: TimeAgo.Suffix
      ) => {
        if (unit === "second") return "just now";
        const plural: string = value !== 1 ? "s" : "";
        return `${value} ${unit}${plural} ${suffix}`;
      }}
    />
  );
};

export const LinkCell: (props: any) => React.ReactNode = (props: any) => {
  return (
    <Link href={props.href} isExternal>
      {props.value}
    </Link>
  );
};

export const DataTable: React.FunctionComponent<DataTableProps> = ({
  data,
  columns,
  pageSize = 10,
  maxW,
  pb,
}) => {
  const {
    getTableProps,
    getTableBodyProps,
    headerGroups,
    rows,
    prepareRow,
    pageOptions,
    canNextPage,
    nextPage,
    canPreviousPage,
    previousPage,
    state: { pageIndex },
  } = useTable(
    { columns: columns as any, data, initialState: { pageIndex: 0, pageSize } },
    useSortBy,
    usePagination
  );

  const last = data.length;
  const start = pageIndex * pageSize + 1;
  const end = Math.min((pageIndex + 1) * pageSize, last);

  return (
    <VStack maxW={maxW} pb={pb}>
      <Table {...getTableProps()} size={"sm"}>
        <Thead>
          {headerGroups.map((headerGroup) => (
            <Tr {...headerGroup.getHeaderGroupProps()}>
              {headerGroup.headers.map((column: any) => (
                <Th
                  {...column.getHeaderProps(column.getSortByToggleProps())}
                  isNumeric={column.isNumeric}
                >
                  {column.render("Header")}
                  <chakra.span pl="4">
                    {column.isSorted ? (
                      column.isSortedDesc ? (
                        <TriangleDownIcon aria-label="sorted descending" />
                      ) : (
                        <TriangleUpIcon aria-label="sorted ascending" />
                      )
                    ) : null}
                  </chakra.span>
                </Th>
              ))}
            </Tr>
          ))}
        </Thead>
        <Tbody {...getTableBodyProps()}>
          {rows.slice(start - 1, end).map((row: any) => {
            prepareRow(row);
            return (
              <Tr {...row.getRowProps()}>
                {row.cells.map((cell: any) => (
                  <Td
                    {...cell.getCellProps()}
                    isNumeric={cell.column.isNumeric}
                  >
                    {cell.render("Cell")}
                  </Td>
                ))}
              </Tr>
            );
          })}
        </Tbody>
      </Table>
      {pageOptions.length > 1 ? (
        <Flex width={"100%"} pr={10} justifyContent={"flex-end"} pt={4}>
          <Text fontSize={"sm"} pr={2}>
            Showing {start} to {end} of {last}.{" "}
          </Text>
          <HiChevronLeft
            color={canPreviousPage ? "black" : "dimgray"}
            onClick={previousPage}
          />
          <HiChevronRight
            color={canNextPage ? "black" : "dimgray"}
            onClick={nextPage}
          />
        </Flex>
      ) : null}
    </VStack>
  );
};
