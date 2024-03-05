# typed: false
# frozen_string_literal: true

# This file was generated by GoReleaser. DO NOT EDIT.
class Blink < Formula
  desc "Open Source stream processing framework"
  homepage "https://github.com/usedatabrew/blink"
  version "1.14.0"

  on_macos do
    url "https://github.com/usedatabrew/blink/releases/download/v1.14.0/blink_1.14.0_darwin_amd64.tar.gz"
    sha256 "955a1246aa3f5560c71b44a2c842eb757762c3bff29cdf2bda05bc02d4e860b6"

    def install
      bin.install "blink"
    end

    if Hardware::CPU.arm?
      def caveats
        <<~EOS
          The darwin_arm64 architecture is not supported for the Blink
          formula at this time. The darwin_amd64 binary may work in compatibility
          mode, but it might not be fully supported.
        EOS
      end
    end
  end

  on_linux do
    if Hardware::CPU.intel?
      url "https://github.com/usedatabrew/blink/releases/download/v1.14.0/blink_1.14.0_linux_amd64.tar.gz"
      sha256 "9bf58c13971b0b5f2fb800f552b137ff73e33195238a04aaf7612886d950d12b"

      def install
        bin.install "blink"
      end
    end
  end
end
