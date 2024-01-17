# typed: false
# frozen_string_literal: true

# This file was generated by GoReleaser. DO NOT EDIT.
class Blink < Formula
  desc "Open Source stream processing framework"
  homepage "https://github.com/usedatabrew/blink"
  version "1.7.0"

  on_macos do
    url "https://github.com/usedatabrew/blink/releases/download/v1.7.0/blink_1.7.0_darwin_amd64.tar.gz"
    sha256 "f79d872531e64a6bfbc2cfd4a5000a5ae7d4d83462bf3952c750b95c2ab6f120"

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
      url "https://github.com/usedatabrew/blink/releases/download/v1.7.0/blink_1.7.0_linux_amd64.tar.gz"
      sha256 "00687259c85a7c3338029abc1d2157ea3411c74568bb442eea1663b65d16c08c"

      def install
        bin.install "blink"
      end
    end
  end
end
