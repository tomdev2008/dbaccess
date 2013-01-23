package com.dajie.core.admin;

import java.util.Scanner;

public class Admin {
	static Scanner consoleInput = new Scanner(System.in);

	static String SEP = "> ";

	static String nextInputString() {
		return consoleInput.nextLine().trim();
	}

	static int nextInputInt() {
		int r = -1;
		try {
			r = Integer.parseInt(consoleInput.nextLine().trim());
		} catch (Exception e) {

		}
		return r;
	}

	static void printMakeSurePrompt(String tip, String message) {
		print(tip, message + " Confirm?[yes/no]: ");
	}

	static void print(String message) {
		System.out.print(message);
	}

	static void print(String tip, String message) {
		System.out.print(tip + SEP + message);
	}

	static void println(String message) {
		System.out.println(message);
	}

	static void println(String tip, String message) {
		System.out.println(tip + SEP + message);
	}

	static void exit() {
		System.exit(0);
	}

	static void printCommands() {
		println("*** Commands ***");
		String formatStr = String.format("%1$12s%2$12s%3$12s%4$12s%5$12s",
				"1: check", "2: diff", "3: commit", "4: quit", "5: help");
		println(formatStr);
		print("What", "");
	}

	static void help() {
		println("");
		println("Help:");
		println(String.format("%1$-12s- %2$-40s", "  check",
				"check local database_desc.json file format and content"));
		println(String.format("%1$-12s- %2$-40s", "  diff",
				"diff content between and local file"));
		println(String.format("%1$-12s- %2$-40s", "  committ",
				"replace online content by local content"));
		println("");
	}

	public static void main(String[] args) {

		// get opt and check input

		while (true) {
			printCommands();
			int select = nextInputInt();
			switch (select) {
			case 1:
				break;
			case 2:
				break;
			case 3:
				break;
			case 4:
				exit();
				break;
			case 5:
				help();
				break;
			default:
				println("Please input correctly! ^_^ GOOD LUCK GEEKER");
				continue;
			}
		}

	}

}
